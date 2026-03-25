package com.pipeline.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.math.BigDecimal
import java.time.Instant

@JsonIgnoreProperties(ignoreUnknown = true)
data class PriceEvent(
    val productId: String,
    val price: BigDecimal,
    val currency: String,
    val updatedAt: Instant
)
