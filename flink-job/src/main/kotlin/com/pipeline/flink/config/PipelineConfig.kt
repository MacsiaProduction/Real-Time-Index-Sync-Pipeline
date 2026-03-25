package com.pipeline.flink.config

import java.io.Serializable
import java.util.Properties

data class PipelineConfig(
    // PostgreSQL
    val pgHost: String,
    val pgPort: Int,
    val pgDatabase: String,
    val pgUsername: String,
    val pgPassword: String,
    val pgSchemaList: String,
    val pgTableList: String,

    // Kafka
    val kafkaBootstrapServers: String,
    val pricesTopic: String,
    val stockTopic: String,
    val dlqTopic: String,
    val kafkaGroupId: String,

    // Elasticsearch
    val esHost: String,
    val esPort: Int,
    val esIndexName: String,

    // Pipeline tuning
    val checkpointIntervalMs: Long,
    val stateTtlHours: Long,
    val esBulkFlushMaxActions: Int,
    val esBulkFlushIntervalMs: Long
) : Serializable {

    companion object {
        fun fromProperties(props: Properties) = PipelineConfig(
            pgHost          = props.getProperty("pg.host", "localhost"),
            pgPort          = props.getProperty("pg.port", "5432").toInt(),
            pgDatabase      = props.getProperty("pg.database", "pipeline"),
            pgUsername      = props.getProperty("pg.username", "pipeline"),
            pgPassword      = props.getProperty("pg.password", "pipeline"),
            pgSchemaList    = props.getProperty("pg.schema.list", "public"),
            pgTableList     = props.getProperty("pg.table.list", "public.products"),
            kafkaBootstrapServers = props.getProperty("kafka.bootstrap.servers", "localhost:9092"),
            pricesTopic     = props.getProperty("kafka.topic.prices", "prices"),
            stockTopic      = props.getProperty("kafka.topic.stock", "stock"),
            dlqTopic        = props.getProperty("kafka.topic.dlq", "dlq"),
            kafkaGroupId    = props.getProperty("kafka.group.id", "rt-index-pipeline"),
            esHost          = props.getProperty("es.host", "localhost"),
            esPort          = props.getProperty("es.port", "9200").toInt(),
            esIndexName     = props.getProperty("es.index.name", "products"),
            checkpointIntervalMs  = props.getProperty("checkpoint.interval.ms", "60000").toLong(),
            stateTtlHours         = props.getProperty("state.ttl.hours", "24").toLong(),
            esBulkFlushMaxActions = props.getProperty("es.bulk.flush.max.actions", "500").toInt(),
            esBulkFlushIntervalMs = props.getProperty("es.bulk.flush.interval.ms", "1000").toLong()
        )

        fun fromEnv(): PipelineConfig {
            val props = Properties()
            System.getenv().forEach { (k, v) ->
                props[k.lowercase().replace('_', '.')] = v
            }
            return fromProperties(props)
        }

        fun load(): PipelineConfig {
            val propsFile = PipelineConfig::class.java.classLoader
                .getResourceAsStream("pipeline.properties")
            val props = Properties()
            if (propsFile != null) {
                propsFile.use { props.load(it) }
            }
            // env перекрывает файл
            System.getenv().forEach { (k, v) ->
                props[k.lowercase().replace('_', '.')] = v
            }
            return fromProperties(props)
        }
    }
}
