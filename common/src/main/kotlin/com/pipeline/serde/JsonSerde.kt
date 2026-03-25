package com.pipeline.serde

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue

object JsonSerde {
    val mapper: ObjectMapper = ObjectMapper().apply {
        registerModule(KotlinModule.Builder().build())
        registerModule(JavaTimeModule())
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    }

    fun serialize(value: Any): ByteArray = mapper.writeValueAsBytes(value)

    fun serializeToString(value: Any): String = mapper.writeValueAsString(value)

    inline fun <reified T> deserialize(bytes: ByteArray): T = mapper.readValue(bytes)

    inline fun <reified T> deserializeString(json: String): T = mapper.readValue(json)
}
