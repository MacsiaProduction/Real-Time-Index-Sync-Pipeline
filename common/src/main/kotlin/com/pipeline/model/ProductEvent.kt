package com.pipeline.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.Instant

@JsonIgnoreProperties(ignoreUnknown = true)
data class ProductEvent(
    val productId: String,
    val name: String,
    val category: String?,
    val brand: String?,
    val description: String?,
    val updatedAt: Instant,
    val operation: String  // "c" = create, "u" = update, "d" = delete
) {
    val isDelete: Boolean get() = operation == "d"
}
