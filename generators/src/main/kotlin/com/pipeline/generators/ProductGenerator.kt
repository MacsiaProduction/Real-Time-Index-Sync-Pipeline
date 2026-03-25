package com.pipeline.generators

import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.random.Random

/** Генератор товаров: INSERT/UPDATE/DELETE в PostgreSQL, CDC подхватывает через WAL. */
class ProductGenerator(private val config: GeneratorConfig) {

    private val log = LoggerFactory.getLogger(ProductGenerator::class.java)

    private val categories = listOf("Electronics", "Clothing", "Books", "Sports", "Home", "Food")
    private val brands = listOf("BrandA", "BrandB", "BrandC", "BrandD", "BrandE")

    private val existingIds: CopyOnWriteArrayList<String> = CopyOnWriteArrayList()
    private var nextIdSeq = 0
    private var conn: Connection? = null
    private val rng = Random.Default

    suspend fun run() {
        conn = DriverManager.getConnection(config.pgJdbcUrl, config.pgUsername, config.pgPassword)
        log.info("ProductGenerator connected to PostgreSQL")

        // подхватить существующие id
        conn!!.createStatement().executeQuery("SELECT product_id FROM products").use { rs ->
            while (rs.next()) {
                val id = rs.getString(1)
                existingIds.add(id)
                val seq = id.removePrefix("product-").toIntOrNull() ?: -1
                if (seq >= nextIdSeq) nextIdSeq = seq + 1
            }
        }
        log.info("ProductGenerator: {} existing products, resuming at product-{}", existingIds.size, nextIdSeq)

        val toInsert = (minOf(config.productPoolSize / 10, 100) - existingIds.size).coerceAtLeast(0)
        repeat(toInsert) { insertProduct(conn!!) }

        var eventCount = 0L
        var burstUntil = 0L

        while (true) {
            val now = System.currentTimeMillis()
            val isBurst = now < burstUntil
            if (!isBurst && eventCount % 6000 == 0L) {
                burstUntil = now + 10_000
                log.info("ProductGenerator starting burst period")
            }

            val roll = rng.nextDouble()
            when {
                roll < config.deletionFraction && existingIds.isNotEmpty() -> deleteProduct(conn!!)
                existingIds.size < config.productPoolSize -> insertProduct(conn!!)
                else -> updateProduct(conn!!)
            }
            eventCount++

            // имитация out-of-order
            val baseDelayMs = (1000L / config.productsRatePerSec).coerceAtLeast(1L)
            if (rng.nextDouble() < config.outOfOrderFraction) {
                delay(rng.nextLong(50, 500))
            } else {
                delay(baseDelayMs)
            }
        }
    }

    private fun insertProduct(conn: Connection) {
        val id = "product-${nextIdSeq++}"
        conn.prepareStatement(
            "INSERT INTO products(product_id, name, category, brand, description, updated_at) " +
            "VALUES (?, ?, ?, ?, ?, ?)"
        ).use { ps ->
            ps.setString(1, id)
            ps.setString(2, "Product ${id.take(8)}")
            ps.setString(3, categories.random(rng))
            ps.setString(4, brands.random(rng))
            ps.setString(5, "Description for ${id.take(8)}")
            ps.setObject(6, java.sql.Timestamp.from(Instant.now()))
            ps.executeUpdate()
        }
        existingIds.add(id)
    }

    private fun updateProduct(conn: Connection) {
        val id = existingIds.random(rng)
        // имитация дубликатов
        val ts = if (rng.nextDouble() < config.duplicateFraction) Instant.now().minusSeconds(1)
                 else Instant.now()
        conn.prepareStatement(
            "UPDATE products SET name = ?, updated_at = ? WHERE product_id = ?"
        ).use { ps ->
            ps.setString(1, "Updated Product ${id.take(8)} v${rng.nextInt(100)}")
            ps.setObject(2, java.sql.Timestamp.from(ts))
            ps.setString(3, id)
            ps.executeUpdate()
        }
    }

    private fun deleteProduct(conn: Connection) {
        if (existingIds.isEmpty()) return
        val id = existingIds.random(rng)
        conn.prepareStatement("DELETE FROM products WHERE product_id = ?").use { ps ->
            ps.setString(1, id)
            ps.executeUpdate()
        }
        existingIds.remove(id)
        log.debug("Deleted product {}", id)
    }

    fun close() {
        conn?.close()
    }
}
