package com.pipeline.flink.sink

import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation
import co.elastic.clients.util.BinaryData
import com.pipeline.flink.config.PipelineConfig
import com.pipeline.model.EnrichedProduct
import com.pipeline.serde.JsonSerde
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8AsyncSink
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8AsyncSinkBuilder
import org.apache.http.HttpHost

object ElasticsearchSinkFactory {

    fun create(config: PipelineConfig): Elasticsearch8AsyncSink<EnrichedProduct> {
        return Elasticsearch8AsyncSinkBuilder<EnrichedProduct>()
            .setHosts(HttpHost(config.esHost, config.esPort))
            .setElementConverter { element, _ ->
                val actionBytes = JsonSerde.mapper.writeValueAsBytes(mapOf(
                    "doc" to JsonSerde.mapper.convertValue(element, Map::class.java),
                    "doc_as_upsert" to true
                ))
                UpdateOperation.of<Any, Any> { u ->
                    u.index(config.esIndexName)
                     .id(element.productId)
                     .binaryAction(BinaryData.of(actionBytes, "application/json"))
                }
            }
            .setMaxBatchSize(config.esBulkFlushMaxActions)
            .setMaxTimeInBufferMS(config.esBulkFlushIntervalMs)
            .build()
    }
}
