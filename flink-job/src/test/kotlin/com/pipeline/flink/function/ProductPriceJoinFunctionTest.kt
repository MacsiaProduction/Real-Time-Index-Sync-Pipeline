package com.pipeline.flink.function

import com.pipeline.model.EnrichedProduct
import com.pipeline.model.PriceEvent
import com.pipeline.model.ProductEvent
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant

class ProductPriceJoinFunctionTest {

    private lateinit var harness: KeyedTwoInputStreamOperatorTestHarness<String, ProductEvent, PriceEvent, EnrichedProduct>

    @BeforeEach
    fun setup() {
        val op = KeyedCoProcessOperator(ProductPriceJoinFunction(24L * 3_600_000))
        harness = KeyedTwoInputStreamOperatorTestHarness(
            op,
            { e: ProductEvent -> e.productId },
            { e: PriceEvent -> e.productId },
            Types.STRING
        )
        harness.open()
    }

    @AfterEach
    fun tearDown() {
        harness.close()
    }

    private fun makeProduct(id: String, name: String, ts: Instant = Instant.now(), op: String = "c") =
        ProductEvent(productId = id, name = name, category = null, brand = null, description = null, updatedAt = ts, operation = op)

    private fun makePrice(id: String, price: BigDecimal, ts: Instant = Instant.now()) =
        PriceEvent(productId = id, price = price, currency = "USD", updatedAt = ts)

    private fun outputDocs(): List<EnrichedProduct> =
        harness.output.filterIsInstance<StreamRecord<*>>().map { it.value as EnrichedProduct }

    @Test
    fun `product first emits partial doc with null price`() {
        harness.processElement1(StreamRecord(makeProduct("p1", "Widget"), 0L))

        val docs = outputDocs()
        assertEquals(1, docs.size)
        assertEquals("p1", docs[0].productId)
        assertEquals("Widget", docs[0].name)
        assertNull(docs[0].price)
        assertEquals(1.0 / 3.0, docs[0].completeness, 0.001)
    }

    @Test
    fun `price first emits partial doc with null name`() {
        harness.processElement2(StreamRecord(makePrice("p1", BigDecimal("9.99")), 0L))

        val docs = outputDocs()
        assertEquals(1, docs.size)
        assertEquals("p1", docs[0].productId)
        assertNull(docs[0].name)
        assertEquals(BigDecimal("9.99"), docs[0].price)
        assertEquals(1.0 / 3.0, docs[0].completeness, 0.001)
    }

    @Test
    fun `product then price emits two docs with increasing completeness`() {
        val t = Instant.now()
        harness.processElement1(StreamRecord(makeProduct("p1", "Widget", t), 0L))
        harness.processElement2(StreamRecord(makePrice("p1", BigDecimal("9.99"), t), 1L))

        val docs = outputDocs()
        assertEquals(2, docs.size)
        assertEquals(1.0 / 3.0, docs[0].completeness, 0.001)
        assertEquals("Widget", docs[1].name)
        assertEquals(BigDecimal("9.99"), docs[1].price)
        assertEquals(2.0 / 3.0, docs[1].completeness, 0.001)
    }

    @Test
    fun `stale product event is ignored`() {
        val t1 = Instant.now()
        val t0 = t1.minusSeconds(10)
        harness.processElement1(StreamRecord(makeProduct("p1", "Newer", t1), 0L))
        harness.processElement1(StreamRecord(makeProduct("p1", "Older", t0), 1L))

        val docs = outputDocs()
        assertEquals(1, docs.size)
        assertEquals("Newer", docs[0].name)
    }

    @Test
    fun `duplicate product with same timestamp is ignored`() {
        val t = Instant.now()
        harness.processElement1(StreamRecord(makeProduct("p1", "Widget", t), 0L))
        harness.processElement1(StreamRecord(makeProduct("p1", "Widget", t), 1L))

        assertEquals(1, outputDocs().size)
    }

    @Test
    fun `stale price event is ignored`() {
        val t1 = Instant.now()
        val t0 = t1.minusSeconds(5)
        harness.processElement2(StreamRecord(makePrice("p1", BigDecimal("9.99"), t1), 0L))
        harness.processElement2(StreamRecord(makePrice("p1", BigDecimal("5.00"), t0), 1L))

        val docs = outputDocs()
        assertEquals(1, docs.size)
        assertEquals(BigDecimal("9.99"), docs[0].price)
    }

    @Test
    fun `delete event clears product state`() {
        val t = Instant.now()
        harness.processElement1(StreamRecord(makeProduct("p1", "Widget", t), 0L))
        harness.processElement2(StreamRecord(makePrice("p1", BigDecimal("9.99"), t), 1L))

        val delete = makeProduct("p1", "", t.plusSeconds(1), "d")
        harness.processElement1(StreamRecord(delete, 2L))

        harness.processElement2(StreamRecord(makePrice("p1", BigDecimal("12.00"), t.plusSeconds(2)), 3L))

        val docs = outputDocs()
        assertEquals(3, docs.size)
        assertNull(docs[2].name)
        assertEquals(BigDecimal("12.00"), docs[2].price)
    }

    @Test
    fun `ttl timer fires and clears product state`() {
        harness.setProcessingTime(0L)
        harness.processElement1(StreamRecord(makeProduct("p1", "Widget"), 0L))

        harness.setProcessingTime(25L * 60 * 60 * 1000)

        harness.processElement2(StreamRecord(makePrice("p1", BigDecimal("9.99")), 1L))

        val docs = outputDocs()
        val last = docs.last()
        assertNull(last.name)
        assertEquals(BigDecimal("9.99"), last.price)
    }

    @Test
    fun `independent keys do not share state`() {
        val t = Instant.now()
        harness.processElement1(StreamRecord(makeProduct("p1", "Alpha", t), 0L))
        harness.processElement2(StreamRecord(makePrice("p2", BigDecimal("1.00"), t), 1L))
        harness.processElement2(StreamRecord(makePrice("p1", BigDecimal("9.99"), t), 2L))

        val docs = outputDocs()
        assertEquals(3, docs.size)
        val p1Final = docs.last { it.productId == "p1" }
        assertEquals("Alpha", p1Final.name)
        assertEquals(BigDecimal("9.99"), p1Final.price)
        val p2Doc = docs.first { it.productId == "p2" }
        assertNull(p2Doc.name)
    }
}
