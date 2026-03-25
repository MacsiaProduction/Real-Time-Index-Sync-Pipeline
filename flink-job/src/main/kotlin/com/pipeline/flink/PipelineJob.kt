package com.pipeline.flink

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
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

object PipelineJob {

    private val log = LoggerFactory.getLogger(PipelineJob::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val pipelineConfig = PipelineConfig.load()
        log.info("Starting RT Index Sync Pipeline")

        val env = StreamExecutionEnvironment.getExecutionEnvironment().apply {
            setParallelism(8)  // 8 слотов; CDC ниже переопределит на 1
            enableCheckpointing(pipelineConfig.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
            restartStrategy = RestartStrategies.fixedDelayRestart(3, 10_000L)
            config.setAutoWatermarkInterval(200)
        }

        val pgSource = PostgresSourceBuilder.PostgresIncrementalSource
            .builder<String>()
            .hostname(pipelineConfig.pgHost)
            .port(pipelineConfig.pgPort)
            .database(pipelineConfig.pgDatabase)
            .schemaList(pipelineConfig.pgSchemaList)
            .tableList(pipelineConfig.pgTableList)
            .username(pipelineConfig.pgUsername)
            .password(pipelineConfig.pgPassword)
            .slotName("flink_rt_pipeline")
            .decodingPluginName("pgoutput")  // стандартный плагин PG 10+
            .deserializer(JsonDebeziumDeserializationSchema(false))
            .build()

        val cdcStream: DataStream<String> = env
            .fromSource(pgSource, WatermarkStrategy.noWatermarks(), "postgres-cdc-source")
            .setParallelism(1)  // CDC single-thread для консистентности снимка

        val productStream: DataStream<ProductEvent> = cdcStream
            .flatMap { json, out: org.apache.flink.util.Collector<ProductEvent> ->
                parseCdcJson(json)?.let { out.collect(it) }
            }
            .name("cdc-product-parser")
            .returns(ProductEvent::class.java)

        val priceSource = KafkaSource.builder<PriceEvent>()
            .setBootstrapServers(pipelineConfig.kafkaBootstrapServers)
            .setTopics(pipelineConfig.pricesTopic)
            .setGroupId(pipelineConfig.kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(PriceEventDeserializer())
            .build()

        val priceStream: DataStream<PriceEvent> = env
            .fromSource(priceSource, WatermarkStrategy.noWatermarks(), "kafka-prices-source")
            .filter { it != null }
            .returns(PriceEvent::class.java)

        val stockSource = KafkaSource.builder<StockEvent>()
            .setBootstrapServers(pipelineConfig.kafkaBootstrapServers)
            .setTopics(pipelineConfig.stockTopic)
            .setGroupId(pipelineConfig.kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(StockEventDeserializer())
            .build()

        val stockStream: DataStream<StockEvent> = env
            .fromSource(stockSource, WatermarkStrategy.noWatermarks(), "kafka-stock-source")
            .filter { it != null }
            .returns(StockEvent::class.java)

        val ttlMs = pipelineConfig.stateTtlHours * 3_600_000L

        val partialDocStream: DataStream<EnrichedProduct> = productStream
            .keyBy { it.productId }
            .connect(priceStream.keyBy { it.productId })
            .process(ProductPriceJoinFunction(ttlMs))
            .name("product-price-join")

        val enrichedStream: DataStream<EnrichedProduct> = partialDocStream
            .keyBy { it.productId }
            .connect(stockStream.keyBy { it.productId })
            .process(EnrichmentJoinFunction(ttlMs))
            .name("enrichment-stock-join")

        val dedupedStream: DataStream<EnrichedProduct> = enrichedStream
            .keyBy { it.productId }
            .process(DeduplicationFunction(ttlMs))
            .name("deduplication")

        val esSink = ElasticsearchSinkFactory.create(pipelineConfig)
        dedupedStream.sinkTo(esSink).name("elasticsearch-sink")

        env.execute("RT Index Sync Pipeline")
    }

    /** Парсит CDC JSON в ProductEvent. null для tombstone, schema-change, невалидных. */
    private fun parseCdcJson(json: String): ProductEvent? {
        return try {
            val root = JsonSerde.mapper.readTree(json)
            val op = root.path("op").asText("")
            if (op.isBlank()) return null  // schema-change / heartbeat

            val row = if (op == "d") root.path("before") else root.path("after")
            if (row.isMissingNode || row.isNull) return null

            val productId = row.path("product_id").asText("")
            if (productId.isBlank()) return null

            val updatedAtMs = row.path("updated_at").asLong(0)
            val updatedAt = if (updatedAtMs > 0) java.time.Instant.ofEpochMilli(updatedAtMs)
                            else java.time.Instant.now()
            val futureToleranceMs = 60_000L
            if (updatedAt.toEpochMilli() > System.currentTimeMillis() + futureToleranceMs) {
                log.warn("CDC event for product {} has future timestamp {}, skipping", productId, updatedAt)
                return null
            }

            ProductEvent(
                productId = productId,
                name = row.path("name").asText(""),
                category = row.path("category").takeUnless { it.isMissingNode || it.isNull }?.asText(),
                brand = row.path("brand").takeUnless { it.isMissingNode || it.isNull }?.asText(),
                description = row.path("description").takeUnless { it.isMissingNode || it.isNull }?.asText(),
                updatedAt = updatedAt,
                operation = op
            )
        } catch (e: Exception) {
            log.error("Failed to parse CDC JSON: {} - {}", e.message, json.take(200))
            null
        }
    }
}
