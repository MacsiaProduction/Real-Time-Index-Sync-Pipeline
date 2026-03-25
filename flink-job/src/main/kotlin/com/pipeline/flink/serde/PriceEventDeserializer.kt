package com.pipeline.flink.serde

import com.fasterxml.jackson.module.kotlin.readValue
import com.pipeline.model.PriceEvent
import com.pipeline.serde.JsonSerde
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.slf4j.LoggerFactory

class PriceEventDeserializer : AbstractDeserializationSchema<PriceEvent>() {

    override fun deserialize(message: ByteArray): PriceEvent? {
        return try {
            val event: PriceEvent = JsonSerde.mapper.readValue(message)
            if (event.productId.isBlank()) {
                log.warn("PriceEvent with blank productId, skipping")
                return null
            }
            val futureToleranceMs = 60_000L
            if (event.updatedAt.toEpochMilli() > System.currentTimeMillis() + futureToleranceMs) {
                log.warn("PriceEvent for {} has future timestamp, skipping", event.productId)
                return null
            }
            event
        } catch (e: Exception) {
            log.error("Failed to deserialize PriceEvent: {}", e.message)
            null
        }
    }

    override fun isEndOfStream(nextElement: PriceEvent?): Boolean = false

    companion object {
        private val log = LoggerFactory.getLogger(PriceEventDeserializer::class.java)
    }
}
