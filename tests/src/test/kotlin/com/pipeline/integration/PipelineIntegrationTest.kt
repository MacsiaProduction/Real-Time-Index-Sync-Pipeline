package com.pipeline.integration

import com.pipeline.flink.config.PipelineConfig
import com.pipeline.flink.function.DeduplicationFunction
import com.pipeline.flink.function.EnrichmentJoinFunction
import com.pipeline.flink.function.ProductPriceJoinFunction
import com.pipeline.flink.serde.PriceEventDeserializer
import com.pipeline.flink.serde.StockEventDeserializer
import com.pipeline.flink.sink.ElasticsearchSinkFactory
import com.pipeline.model.EnrichedProduct
import com.pipeline.model.PriceEvent
import com.pipeline.model.ProductEvent
import com.pipeline.model.StockEvent
import com.pipeline.serde.JsonSerde
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.jupiter.api.*
import java.math.BigDecimal
import java.time.Instant

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class PipelineIntegrationTest {

    companion object {
        private const val INDEX = "products-test"

        @BeforeAll
        @JvmStatic
        fun startContainers() {
            Containers.startAll()
        }

        @AfterAll
        @JvmStatic
        fun stopContainers() {
            Containers.stopAll()
        }
    }

    private lateinit var producer: TestEventProducer
    private lateinit var verifier: ElasticsearchVerifier

    @BeforeEach
    fun setup() {
        producer = TestEventProducer(Containers.kafka.bootstrapServers)
        verifier = ElasticsearchVerifier(
            "localhost",
            Containers.elasticsearch.getMappedPort(9200)
        )
        verifier.ensureIndex(INDEX)
    }

    @AfterEach
    fun tearDown() {
        producer.close()
        verifier.close()
    }

    private fun buildAndExecuteAsync(): org.apache.flink.core.execution.JobClient {
        val config = PipelineConfig(
            pgHost = "localhost", pgPort = 5432,
            pgDatabase = "pipeline", pgUsername = "pipeline", pgPassword = "pipeline",
            pgSchemaList = "public", pgTableList = "public.products",
            kafkaBootstrapServers = Containers.kafka.bootstrapServers,
            pricesTopic = Topics.PRICES, stockTopic = Topics.STOCK, dlqTopic = Topics.DLQ,
            kafkaGroupId = "it-group-${System.currentTimeMillis()}",
            esHost = "localhost", esPort = Containers.elasticsearch.getMappedPort(9200),
            esIndexName = INDEX,
            checkpointIntervalMs = 5_000L,
            stateTtlHours = 24,
            esBulkFlushMaxActions = 10,
            esBulkFlushIntervalMs = 200L
        )

        val env = StreamExecutionEnvironment.createLocalEnvironment(2)
        env.config.setAutoWatermarkInterval(100)

        // тест: ProductEvent напрямую, без CDC envelope
        val productSource = KafkaSource.builder<ProductEvent>()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setTopics(Topics.PRODUCTS)
            .setGroupId(config.kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(object : AbstractDeserializationSchema<ProductEvent>() {
                override fun deserialize(message: ByteArray): ProductEvent? =
                    try { JsonSerde.mapper.readValue(message, ProductEvent::class.java) }
                    catch (_: Exception) { null }
            })
            .build()

        val priceSource = KafkaSource.builder<PriceEvent>()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setTopics(config.pricesTopic)
            .setGroupId(config.kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(PriceEventDeserializer())
            .build()

        val stockSource = KafkaSource.builder<StockEvent>()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setTopics(config.stockTopic)
            .setGroupId(config.kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(StockEventDeserializer())
            .build()

        val productStream: DataStream<ProductEvent> = env
            .fromSource(productSource, WatermarkStrategy.noWatermarks(), "products")
            .filter { it != null }.returns(ProductEvent::class.java)

        val priceStream: DataStream<PriceEvent> = env
            .fromSource(priceSource, WatermarkStrategy.noWatermarks(), "prices")
            .filter { it != null }.returns(PriceEvent::class.java)

        val stockStream: DataStream<StockEvent> = env
            .fromSource(stockSource, WatermarkStrategy.noWatermarks(), "stock")
            .filter { it != null }.returns(StockEvent::class.java)

        val ttlMs = config.stateTtlHours * 3_600_000L

        val partialStream: DataStream<EnrichedProduct> = productStream
            .keyBy { it.productId }
            .connect(priceStream.keyBy { it.productId })
            .process(ProductPriceJoinFunction(ttlMs))

        val enrichedStream: DataStream<EnrichedProduct> = partialStream
            .keyBy { it.productId }
            .connect(stockStream.keyBy { it.productId })
            .process(EnrichmentJoinFunction(ttlMs))

        val dedupedStream: DataStream<EnrichedProduct> = enrichedStream
            .keyBy { it.productId }
            .process(DeduplicationFunction(ttlMs))

        dedupedStream.sinkTo(ElasticsearchSinkFactory.create(config))

        return env.executeAsync("integration-test-pipeline")
    }

    @Test
    @Order(1)
    fun `happy path - all 3 streams produce completeness 1`() {
        val jobClient = buildAndExecuteAsync()
        try {
            val t = Instant.now()
            producer.sendProduct(producer.product("it-p1", "Integration Widget", t))
            producer.sendPrice(producer.price("it-p1", BigDecimal("29.99"), t))
            producer.sendStock(producer.stock("it-p1", "wh1", 150, t))

            val doc = verifier.awaitCompleteness(INDEX, "it-p1", 1.0, timeoutMs = 30_000L)

            Assertions.assertEquals("Integration Widget", doc.name)
            Assertions.assertEquals(BigDecimal("29.99"), doc.price)
            Assertions.assertEquals(150, doc.totalStock)
            Assertions.assertEquals(1.0, doc.completeness, 0.001)
        } finally {
            jobClient.cancel()
        }
    }

    @Test
    @Order(2)
    fun `partial enrichment - product only, completeness 1-3`() {
        val jobClient = buildAndExecuteAsync()
        try {
            producer.sendProduct(producer.product("it-p2", "Partial Widget"))

            val doc = verifier.awaitDocument(INDEX, "it-p2", timeoutMs = 30_000L)

            Assertions.assertEquals("Partial Widget", doc.name)
            Assertions.assertNull(doc.price)
            Assertions.assertNull(doc.totalStock)
            Assertions.assertTrue(doc.completeness < 1.0)
        } finally {
            jobClient.cancel()
        }
    }

    @Test
    @Order(3)
    fun `update propagation - price update reflected in ES`() {
        val jobClient = buildAndExecuteAsync()
        try {
            val t = Instant.now()
            producer.sendProduct(producer.product("it-p3", "Update Widget", t))
            producer.sendPrice(producer.price("it-p3", BigDecimal("9.99"), t))
            verifier.awaitCompleteness(INDEX, "it-p3", 2.0 / 3.0)

            producer.sendPrice(producer.price("it-p3", BigDecimal("14.99"), t.plusSeconds(1)))

            val deadline = System.currentTimeMillis() + 30_000L
            var updatedDoc: EsDoc? = null
            while (System.currentTimeMillis() < deadline) {
                val d = verifier.getDocument(INDEX, "it-p3")
                if (d?.price == BigDecimal("14.99")) { updatedDoc = d; break }
                Thread.sleep(500)
            }

            Assertions.assertNotNull(updatedDoc, "Updated price should appear in ES within 30s")
            Assertions.assertEquals(BigDecimal("14.99"), updatedDoc!!.price)
        } finally {
            jobClient.cancel()
        }
    }

    @Test
    @Order(4)
    fun `multi-warehouse stock sums correctly`() {
        val jobClient = buildAndExecuteAsync()
        try {
            val t = Instant.now()
            producer.sendProduct(producer.product("it-p4", "Multi-WH Widget", t))
            producer.sendPrice(producer.price("it-p4", BigDecimal("5.00"), t))
            producer.sendStock(producer.stock("it-p4", "wh1", 100, t))
            producer.sendStock(producer.stock("it-p4", "wh2", 200, t.plusMillis(1)))
            producer.sendStock(producer.stock("it-p4", "wh3", 50, t.plusMillis(2)))

            val doc = verifier.awaitCompleteness(INDEX, "it-p4", 1.0, timeoutMs = 30_000L)

            Assertions.assertEquals(350, doc.totalStock)
        } finally {
            jobClient.cancel()
        }
    }

    @Test
    @Order(5)
    fun `malformed price event - pipeline continues processing valid events`() {
        val jobClient = buildAndExecuteAsync()
        try {
            producer.sendRawToTopic(Topics.PRICES, "bad-key", "{{not json}}".toByteArray())

            val t = Instant.now()
            producer.sendProduct(producer.product("it-p5", "Resilient Widget", t))
            producer.sendPrice(producer.price("it-p5", BigDecimal("19.99"), t))

            val doc = verifier.awaitCompleteness(INDEX, "it-p5", 2.0 / 3.0, timeoutMs = 30_000L)
            Assertions.assertEquals("Resilient Widget", doc.name)
            Assertions.assertEquals(BigDecimal("19.99"), doc.price)
        } finally {
            jobClient.cancel()
        }
    }

    @Test
    @Order(6)
    fun `throughput smoke - 100 products indexed within 45s`() {
        val jobClient = buildAndExecuteAsync()
        try {
            val t = Instant.now()
            for (i in 1..100) {
                val id = "smoke-$i"
                producer.sendProduct(producer.product(id, "Smoke Product $i", t))
                producer.sendPrice(producer.price(id, BigDecimal("${i}.99"), t))
                producer.sendStock(producer.stock(id, "wh1", i * 10, t))
            }

            val last = verifier.awaitDocument(INDEX, "smoke-100", timeoutMs = 45_000L)
            Assertions.assertNotNull(last)
            Assertions.assertEquals(1.0, last.completeness, 0.001)
        } finally {
            jobClient.cancel()
        }
    }

    @Test
    @Order(7)
    fun `out-of-order events - stale update does not overwrite newer state`() {
        val jobClient = buildAndExecuteAsync()
        try {
            val t = Instant.now()
            producer.sendPrice(producer.price("it-p7", BigDecimal("99.99"), t.plusSeconds(10)))
            producer.sendProduct(producer.product("it-p7", "OOO Widget", t))
            producer.sendPrice(producer.price("it-p7", BigDecimal("1.00"), t.minusSeconds(10)))

            val doc = verifier.awaitCompleteness(INDEX, "it-p7", 2.0 / 3.0, timeoutMs = 30_000L)
            Assertions.assertEquals(BigDecimal("99.99"), doc.price)
        } finally {
            jobClient.cancel()
        }
    }

    @Test
    @Order(8)
    fun `high volume - 1000 products indexed with latency under 10s`() {
        val jobClient = buildAndExecuteAsync()
        try {
            val t = Instant.now()
            for (i in 1..1000) {
                val id = "vol-$i"
                producer.sendProduct(producer.product(id, "Volume Product $i", t))
                producer.sendPrice(producer.price(id, BigDecimal("${(i % 100) + 1}.99"), t))
                producer.sendStock(producer.stock(id, "wh1", i * 5, t))
            }

            verifier.awaitCompleteness(INDEX, "vol-1000", 1.0, timeoutMs = 90_000L)

            val maxLatencyMs = (1..10).mapNotNull { i ->
                val doc = verifier.getDocument(INDEX, "vol-${i * 100}") ?: return@mapNotNull null
                val eventTime = maxOf(
                    doc.lastProductUpdate ?: Instant.EPOCH,
                    doc.lastPriceUpdate ?: Instant.EPOCH,
                    doc.lastStockUpdate ?: Instant.EPOCH
                )
                val indexedAt = doc.indexedAt ?: return@mapNotNull null
                java.time.Duration.between(eventTime, indexedAt).toMillis()
            }.maxOrNull() ?: 0L

            Assertions.assertTrue(maxLatencyMs < 10_000,
                "Max sampled latency ${maxLatencyMs}ms exceeds 10s threshold")
        } finally {
            jobClient.cancel()
        }
    }

    @Test
    @Order(9)
    fun `sustained throughput - price updates reflected within 30s`() {
        val jobClient = buildAndExecuteAsync()
        try {
            for (i in 1..200) {
                val t = Instant.now()
                producer.sendProduct(producer.product("sust-$i", "Sustained Product $i", t))
                producer.sendPrice(producer.price("sust-$i", BigDecimal("$i.99"), t))
                producer.sendStock(producer.stock("sust-$i", "wh1", i, t))
            }
            verifier.awaitCompleteness(INDEX, "sust-200", 1.0, timeoutMs = 60_000L)

            val updateTs = Instant.now()
            for (i in 1..100) {
                producer.sendPrice(producer.price("sust-$i", BigDecimal("${i + 500}.99"), updateTs))
            }

            val deadline = System.currentTimeMillis() + 30_000L
            while (System.currentTimeMillis() < deadline) {
                val doc = verifier.getDocument(INDEX, "sust-1")
                if (doc?.price?.compareTo(BigDecimal("501.99")) == 0) break
                Thread.sleep(500)
            }
            val doc = verifier.getDocument(INDEX, "sust-1")
            Assertions.assertEquals(0, doc?.price?.compareTo(BigDecimal("501.99")),
                "Price update for sust-1 should reflect 501.99 within 30s, got ${doc?.price}")
        } finally {
            jobClient.cancel()
        }
    }
}
