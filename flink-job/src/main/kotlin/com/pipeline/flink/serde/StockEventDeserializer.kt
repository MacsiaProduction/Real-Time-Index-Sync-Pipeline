package com.pipeline.flink.serde

import com.fasterxml.jackson.module.kotlin.readValue
import com.pipeline.model.StockEvent
import com.pipeline.serde.JsonSerde
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.slf4j.LoggerFactory

class StockEventDeserializer : AbstractDeserializationSchema<StockEvent>() {

    override fun deserialize(message: ByteArray): StockEvent? {
        return try {
            val event: StockEvent = JsonSerde.mapper.readValue(message)
            if (event.productId.isBlank()) {
                log.warn("StockEvent with blank productId, skipping")
                return null
            }
            if (event.warehouseId.isBlank()) {
                log.warn("StockEvent with blank warehouseId for {}, skipping", event.productId)
                return null
            }
            val futureToleranceMs = 60_000L
            if (event.updatedAt.toEpochMilli() > System.currentTimeMillis() + futureToleranceMs) {
                log.warn("StockEvent for {} has future timestamp, skipping", event.productId)
                return null
            }
            event
        } catch (e: Exception) {
            log.error("Failed to deserialize StockEvent: {}", e.message)
            null
        }
    }

    override fun isEndOfStream(nextElement: StockEvent?): Boolean = false

    companion object {
        private val log = LoggerFactory.getLogger(StockEventDeserializer::class.java)
    }
}
