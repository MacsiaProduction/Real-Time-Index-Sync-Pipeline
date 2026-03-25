package com.pipeline.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.math.BigDecimal
import java.time.Instant

@JsonIgnoreProperties(ignoreUnknown = true)
data class EnrichedProduct(
    val productId: String,
    val name: String?,
    val category: String?,
    val brand: String?,
    val description: String?,
    val price: BigDecimal?,
    val currency: String?,
    val totalStock: Int?,
    val stockByWarehouse: Map<String, Int>,
    val lastProductUpdate: Instant?,
    val lastPriceUpdate: Instant?,
    val lastStockUpdate: Instant?,
    /** Доля заполненных полей: 0.0–1.0 */
    val completeness: Double,
    val indexedAt: Instant
) {
    companion object {
        fun computeCompleteness(
            name: String?,
            price: BigDecimal?,
            totalStock: Int?
        ): Double {
            val present = listOf(name, price, totalStock).count { it != null }
            return present.toDouble() / 3.0
        }
    }
}
