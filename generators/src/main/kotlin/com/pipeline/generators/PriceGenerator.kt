package com.pipeline.generators

import com.pipeline.model.PriceEvent
import com.pipeline.serde.JsonSerde
import kotlinx.coroutines.delay
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant
import java.util.Properties
import kotlin.random.Random

class PriceGenerator(private val config: GeneratorConfig) {

    private val log = LoggerFactory.getLogger(PriceGenerator::class.java)

    private val currencies = listOf("USD", "EUR", "GBP")
    private val rng = Random.Default

    // random walk по цене
    private val productPrices = mutableMapOf<String, BigDecimal>()

    private val producer = createProducer()

    fun close() { producer.flush(); producer.close() }

    suspend fun run() {
        log.info("PriceGenerator started, targeting {} events/sec", config.pricesRatePerSec)

        var eventCount = 0L
        val delayMs = 1000L / config.pricesRatePerSec
        var burstUntil = 0L

        while (true) {
            val now = System.currentTimeMillis()
            val isBurst = now < burstUntil
            if (!isBurst && eventCount % (config.pricesRatePerSec * 60L) == 0L && eventCount > 0) {
                burstUntil = now + 10_000
                log.info("PriceGenerator burst period starting")
            }

            val productId = "product-${rng.nextInt(config.productPoolSize)}"
            val price = nextPrice(productId)
            val event = PriceEvent(
                productId = productId,
                price = price,
                currency = currencies.random(rng),
                updatedAt = Instant.now()
            )

            val json = JsonSerde.mapper.writeValueAsString(event)
            val record = ProducerRecord<String, String>(config.pricesTopic, productId, json)
            producer.send(record)

            // дубликат
            if (rng.nextDouble() < config.duplicateFraction) {
                producer.send(record)
            }

            eventCount++
            val actualDelay = if (isBurst) (delayMs / config.burstMultiplier).coerceAtLeast(1) else delayMs
            delay(actualDelay)
        }
    }

    private fun nextPrice(productId: String): BigDecimal {
        val current = productPrices.getOrPut(productId) {
            BigDecimal(10 + rng.nextDouble() * 990).setScale(2, RoundingMode.HALF_UP)
        }
        // ±5% за обновление
        val delta = current * BigDecimal(rng.nextDouble() * 0.1 - 0.05)
        val next = (current + delta).coerceAtLeast(BigDecimal("0.01")).setScale(2, RoundingMode.HALF_UP)
        productPrices[productId] = next
        return next
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
