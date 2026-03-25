package com.pipeline.flink.function

import com.pipeline.model.EnrichedProduct
import com.pipeline.model.StockEvent
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

class EnrichmentJoinFunctionTest {

    private lateinit var harness: KeyedTwoInputStreamOperatorTestHarness<String, EnrichedProduct, StockEvent, EnrichedProduct>

    @BeforeEach
    fun setup() {
        val op = KeyedCoProcessOperator(EnrichmentJoinFunction(24L * 3_600_000))
        harness = KeyedTwoInputStreamOperatorTestHarness(
            op,
            { e: EnrichedProduct -> e.productId },
            { e: StockEvent -> e.productId },
            Types.STRING
        )
        harness.open()
    }

    @AfterEach
    fun tearDown() {
        harness.close()
    }

    private fun makePartial(id: String, name: String? = "Product", price: BigDecimal? = null): EnrichedProduct =
        EnrichedProduct(
            productId = id, name = name, category = null, brand = null, description = null,
            price = price, currency = if (price != null) "USD" else null,
            totalStock = null, stockByWarehouse = emptyMap(),
            lastProductUpdate = Instant.now(), lastPriceUpdate = null, lastStockUpdate = null,
            completeness = EnrichedProduct.computeCompleteness(name, price, null),
            indexedAt = Instant.now()
        )

    private fun makeStock(productId: String, warehouseId: String, qty: Int, ts: Instant = Instant.now()) =
        StockEvent(productId = productId, warehouseId = warehouseId, quantity = qty, updatedAt = ts)

    private fun outputDocs(): List<EnrichedProduct> =
        harness.output.filterIsInstance<StreamRecord<*>>().map { it.value as EnrichedProduct }

    @Test
    fun `partial doc first emits with null stock`() {
        harness.processElement1(StreamRecord(makePartial("p1", "Widget"), 0L))

        val docs = outputDocs()
        assertEquals(1, docs.size)
        assertNull(docs[0].totalStock)
        assertTrue(docs[0].stockByWarehouse.isEmpty())
    }

    @Test
    fun `stock first emits with null product name`() {
        harness.processElement2(StreamRecord(makeStock("p1", "wh1", 100), 0L))

        val docs = outputDocs()
        assertEquals(1, docs.size)
        assertEquals(100, docs[0].totalStock)
        assertNull(docs[0].name)
    }

    @Test
    fun `partial doc then stock emits full enrichment`() {
        val t = Instant.now()
        harness.processElement1(StreamRecord(makePartial("p1", "Widget", BigDecimal("9.99")), 0L))
        harness.processElement2(StreamRecord(makeStock("p1", "wh1", 100, t), 1L))

        val docs = outputDocs()
        assertEquals(2, docs.size)
        val last = docs.last()
        assertEquals("Widget", last.name)
        assertEquals(BigDecimal("9.99"), last.price)
        assertEquals(100, last.totalStock)
        assertEquals(1.0, last.completeness, 0.001)
    }

    @Test
    fun `multiple warehouses are summed for total stock`() {
        val t = Instant.now()
        harness.processElement1(StreamRecord(makePartial("p1"), 0L))
        harness.processElement2(StreamRecord(makeStock("p1", "wh1", 100, t), 1L))
        harness.processElement2(StreamRecord(makeStock("p1", "wh2", 50, t.plusMillis(1)), 2L))
        harness.processElement2(StreamRecord(makeStock("p1", "wh3", 200, t.plusMillis(2)), 3L))

        val last = outputDocs().last()
        assertEquals(350, last.totalStock)
        assertEquals(3, last.stockByWarehouse.size)
        assertEquals(100, last.stockByWarehouse["wh1"])
        assertEquals(50, last.stockByWarehouse["wh2"])
        assertEquals(200, last.stockByWarehouse["wh3"])
    }

    @Test
    fun `warehouse update overrides previous quantity`() {
        val t = Instant.now()
        harness.processElement2(StreamRecord(makeStock("p1", "wh1", 100, t), 0L))
        harness.processElement2(StreamRecord(makeStock("p1", "wh1", 75, t.plusSeconds(1)), 1L))

        val docs = outputDocs()
        assertEquals(2, docs.size)
        assertEquals(75, docs.last().totalStock)
    }

    @Test
    fun `out-of-order stock events for same warehouse overwrites previous`() {
        val t1 = Instant.now()
        val t0 = t1.minusSeconds(10)
        harness.processElement2(StreamRecord(makeStock("p1", "wh1", 100, t1), 0L))
        harness.processElement2(StreamRecord(makeStock("p1", "wh1", 50, t0), 1L))

        val docs = outputDocs()
        assertEquals(2, docs.size)
        assertEquals(50, docs.last().totalStock)
    }

    @Test
    fun `partial doc update is reflected in merged output`() {
        val t = Instant.now()
        harness.processElement2(StreamRecord(makeStock("p1", "wh1", 100, t), 0L))

        val withPrice = makePartial("p1", "Widget", BigDecimal("14.99"))
        harness.processElement1(StreamRecord(withPrice, 1L))

        val last = outputDocs().last()
        assertEquals("Widget", last.name)
        assertEquals(BigDecimal("14.99"), last.price)
        assertEquals(100, last.totalStock)
    }

    @Test
    fun `completeness is one third when only stock present`() {
        harness.processElement2(StreamRecord(makeStock("p1", "wh1", 0, Instant.now()), 0L))

        val doc = outputDocs().first()
        assertEquals(1.0 / 3.0, doc.completeness, 0.001)
    }

    @Test
    fun `lastStockUpdate reflects actual event timestamp`() {
        val t = Instant.now().minusSeconds(30)
        harness.processElement2(StreamRecord(makeStock("p1", "wh1", 100, t), 0L))

        val doc = outputDocs().first()
        assertEquals(t.toEpochMilli(), doc.lastStockUpdate?.toEpochMilli())
    }

    @Test
    fun `ttl timer clears state`() {
        harness.setProcessingTime(0L)
        harness.processElement1(StreamRecord(makePartial("p1", "Widget"), 0L))
        harness.processElement2(StreamRecord(makeStock("p1", "wh1", 100, Instant.now()), 1L))

        harness.setProcessingTime(25L * 60 * 60 * 1000)

        harness.processElement1(StreamRecord(makePartial("p1", "Widget v2"), 2L))

        val last = outputDocs().last()
        assertEquals("Widget v2", last.name)
        assertNull(last.totalStock)
        assertTrue(last.stockByWarehouse.isEmpty())
    }
}
