package com.pipeline.generators

data class GeneratorConfig(
    val kafkaBootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092",
    val pgJdbcUrl: String            = System.getenv("PG_JDBC_URL") ?: "jdbc:postgresql://localhost:5432/pipeline",
    val pgUsername: String           = System.getenv("PG_USERNAME") ?: "pipeline",
    val pgPassword: String           = System.getenv("PG_PASSWORD") ?: "pipeline",
    val pricesTopic: String          = System.getenv("KAFKA_TOPIC_PRICES") ?: "prices",
    val stockTopic: String           = System.getenv("KAFKA_TOPIC_STOCK") ?: "stock",
    val productPoolSize: Int         = System.getenv("PRODUCT_POOL_SIZE")?.toInt() ?: 1000,
    val productsRatePerSec: Int      = System.getenv("PRODUCTS_RATE_PER_SEC")?.toInt() ?: 50,
    val pricesRatePerSec: Int        = System.getenv("PRICES_RATE_PER_SEC")?.toInt() ?: 2000,
    val stockRatePerSec: Int         = System.getenv("STOCK_RATE_PER_SEC")?.toInt() ?: 2000,
    val warehouseCount: Int          = System.getenv("WAREHOUSE_COUNT")?.toInt() ?: 5,
    /** Доля событий с задержкой (имитация out-of-order) */
    val outOfOrderFraction: Double   = System.getenv("OUT_OF_ORDER_FRACTION")?.toDouble() ?: 0.05,
    /** Доля дубликатов */
    val duplicateFraction: Double    = System.getenv("DUPLICATE_FRACTION")?.toDouble() ?: 0.02,
    /** Доля удалений */
    val deletionFraction: Double     = System.getenv("DELETION_FRACTION")?.toDouble() ?: 0.001,
    /** Множитель нагрузки (burst каждые 60с на 10с) */
    val burstMultiplier: Int         = System.getenv("BURST_MULTIPLIER")?.toInt() ?: 3
)
