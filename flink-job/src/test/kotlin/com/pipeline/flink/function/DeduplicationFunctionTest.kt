package com.pipeline.flink.function

import com.pipeline.model.EnrichedProduct
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant

class DeduplicationFunctionTest {

    private lateinit var harness: KeyedOneInputStreamOperatorTestHarness<String, EnrichedProduct, EnrichedProduct>

    @BeforeEach
    fun setup() {
        val op = KeyedProcessOperator(DeduplicationFunction(24L * 3_600_000))
        harness = KeyedOneInputStreamOperatorTestHarness(
            op,
            { e: EnrichedProduct -> e.productId },
            Types.STRING
        )
        harness.open()
    }

    @AfterEach
    fun tearDown() {
        harness.close()
    }

    private fun makeDoc(
        id: String,
        name: String = "Product",
        price: BigDecimal? = BigDecimal("9.99"),
        stock: Int? = 100,
        indexedAt: Instant = Instant.now()
    ) = EnrichedProduct(
        productId = id, name = name, category = null, brand = null, description = null,
        price = price, currency = if (price != null) "USD" else null,
        totalStock = stock, stockByWarehouse = emptyMap(),
        lastProductUpdate = null, lastPriceUpdate = null, lastStockUpdate = null,
        completeness = EnrichedProduct.computeCompleteness(name, price, stock),
        indexedAt = indexedAt
    )

    private fun outputDocs(): List<EnrichedProduct> =
        harness.output.filterIsInstance<StreamRecord<*>>().map { it.value as EnrichedProduct }

    @Test
    fun `first event always passes through`() {
        harness.processElement(StreamRecord(makeDoc("p1"), 0L))

        assertEquals(1, outputDocs().size)
    }

    @Test
    fun `identical event is filtered`() {
        val doc = makeDoc("p1")
        harness.processElement(StreamRecord(doc, 0L))
        harness.processElement(StreamRecord(doc.copy(indexedAt = Instant.now().plusSeconds(1)), 1L))

        assertEquals(1, outputDocs().size)
    }

    @Test
    fun `changed price passes through`() {
        harness.processElement(StreamRecord(makeDoc("p1", price = BigDecimal("9.99")), 0L))
        harness.processElement(StreamRecord(makeDoc("p1", price = BigDecimal("12.99")), 1L))

        assertEquals(2, outputDocs().size)
    }

    @Test
    fun `changed stock passes through`() {
        harness.processElement(StreamRecord(makeDoc("p1", stock = 100), 0L))
        harness.processElement(StreamRecord(makeDoc("p1", stock = 50), 1L))

        assertEquals(2, outputDocs().size)
    }

    @Test
    fun `changed name passes through`() {
        harness.processElement(StreamRecord(makeDoc("p1", name = "Old Name"), 0L))
        harness.processElement(StreamRecord(makeDoc("p1", name = "New Name"), 1L))

        assertEquals(2, outputDocs().size)
        assertEquals("New Name", outputDocs()[1].name)
    }

    @Test
    fun `indexedAt difference alone does not create new hash`() {
        val t = Instant.now()
        harness.processElement(StreamRecord(makeDoc("p1", indexedAt = t), 0L))
        harness.processElement(StreamRecord(makeDoc("p1", indexedAt = t.plusSeconds(60)), 1L))

        assertEquals(1, outputDocs().size)
    }

    @Test
    fun `null fields change hash`() {
        harness.processElement(StreamRecord(makeDoc("p1", price = BigDecimal("9.99"), stock = 100), 0L))
        harness.processElement(StreamRecord(makeDoc("p1", price = null, stock = 100), 1L))

        assertEquals(2, outputDocs().size)
    }

    @Test
    fun `alternating changes all pass through`() {
        harness.processElement(StreamRecord(makeDoc("p1", price = BigDecimal("9.99")), 0L))
        harness.processElement(StreamRecord(makeDoc("p1", price = BigDecimal("12.00")), 1L))
        harness.processElement(StreamRecord(makeDoc("p1", price = BigDecimal("9.99")), 2L))

        assertEquals(3, outputDocs().size)
    }

    @Test
    fun `different product keys are independent`() {
        val doc1 = makeDoc("p1", name = "Alpha")
        val doc2 = makeDoc("p2", name = "Beta")
        harness.processElement(StreamRecord(doc1, 0L))
        harness.processElement(StreamRecord(doc1.copy(indexedAt = Instant.now().plusSeconds(1)), 1L))
        harness.processElement(StreamRecord(doc2, 2L))
        harness.processElement(StreamRecord(doc2.copy(indexedAt = Instant.now().plusSeconds(1)), 3L))

        assertEquals(2, outputDocs().size)
    }

    @Test
    fun `completeness change passes through`() {
        harness.processElement(StreamRecord(makeDoc("p1", price = null, stock = 100), 0L))
        harness.processElement(StreamRecord(makeDoc("p1", price = BigDecimal("9.99"), stock = 100), 1L))

        assertEquals(2, outputDocs().size)
    }
}
