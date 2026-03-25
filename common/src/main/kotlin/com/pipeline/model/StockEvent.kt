package com.pipeline.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.Instant

@JsonIgnoreProperties(ignoreUnknown = true)
data class StockEvent(
    val productId: String,
    val warehouseId: String,
    val quantity: Int,
    val updatedAt: Instant
)
