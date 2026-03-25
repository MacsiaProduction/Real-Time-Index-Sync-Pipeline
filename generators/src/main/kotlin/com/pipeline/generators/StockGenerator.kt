package com.pipeline.generators

import com.pipeline.model.StockEvent
import com.pipeline.serde.JsonSerde
import kotlinx.coroutines.delay
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.Properties
import kotlin.random.Random

class StockGenerator(private val config: GeneratorConfig) {

    private val log = LoggerFactory.getLogger(StockGenerator::class.java)
    private val rng = Random.Default

    private val stockLevels = mutableMapOf<String, Int>()

    private val producer = createProducer()

    fun close() { producer.flush(); producer.close() }

    suspend fun run() {
        log.info("StockGenerator started, targeting {} events/sec", config.stockRatePerSec)

        var eventCount = 0L
        val delayMs = 1000L / config.stockRatePerSec
        var burstUntil = 0L

        while (true) {
            val now = System.currentTimeMillis()
            val isBurst = now < burstUntil
            if (!isBurst && eventCount % (config.stockRatePerSec * 60L) == 0L && eventCount > 0) {
                burstUntil = now + 10_000
                log.info("StockGenerator burst period starting")
            }

            val productId = "product-${rng.nextInt(config.productPoolSize)}"
            val warehouseId = "warehouse-${rng.nextInt(config.warehouseCount)}"
            val key = "$productId:$warehouseId"

            val currentQty = stockLevels.getOrPut(key) { rng.nextInt(0, 500) }
            val delta = rng.nextInt(-20, 21)
            val newQty = (currentQty + delta).coerceAtLeast(0)
            stockLevels[key] = newQty

            val event = StockEvent(
                productId = productId,
                warehouseId = warehouseId,
                quantity = newQty,
                updatedAt = Instant.now()
            )

            val json = JsonSerde.mapper.writeValueAsString(event)
            val record = ProducerRecord<String, String>(config.stockTopic, productId, json)
            producer.send(record)

            if (rng.nextDouble() < config.duplicateFraction) {
                producer.send(record)
            }

            eventCount++
            val actualDelay = if (isBurst) (delayMs / config.burstMultiplier).coerceAtLeast(1) else delayMs
            delay(actualDelay)
        }
    }

    private fun createProducer(): KafkaProducer<String, String> {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "1")
            put(ProducerConfig.LINGER_MS_CONFIG, "5")
            put(ProducerConfig.BATCH_SIZE_CONFIG, "65536")
        }
        return KafkaProducer(props)
    }
}
