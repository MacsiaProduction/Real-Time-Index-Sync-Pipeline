package com.pipeline.integration

import com.pipeline.model.PriceEvent
import com.pipeline.model.ProductEvent
import com.pipeline.model.StockEvent
import com.pipeline.serde.JsonSerde
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName
import java.math.BigDecimal
import java.time.Instant
import java.util.Properties

object Topics {
    const val PRODUCTS = "products"
    const val PRICES   = "prices"
    const val STOCK    = "stock"
    const val DLQ      = "dlq"
}

object Containers {

    val kafka: KafkaContainer = KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    ).withKraft()

    val elasticsearch: ElasticsearchContainer = ElasticsearchContainer(
        DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.17.0")
            .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch")
    ).withEnv("xpack.security.enabled", "false")
     .withEnv("discovery.type", "single-node")
     .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
     .waitingFor(Wait.forHttp("/_cluster/health").forStatusCode(200))

    fun startAll() {
        if (!kafka.isRunning) kafka.start()
        if (!elasticsearch.isRunning) elasticsearch.start()
        ensureTopics()
    }

    fun stopAll() {
        elasticsearch.stop()
        kafka.stop()
    }

    private fun ensureTopics() {
        val adminProps = Properties().apply {
            put("bootstrap.servers", kafka.bootstrapServers)
        }
        AdminClient.create(adminProps).use { admin ->
            val existing = admin.listTopics().names().get()
            val toCreate = listOf(Topics.PRODUCTS, Topics.PRICES, Topics.STOCK, Topics.DLQ)
                .filter { it !in existing }
                .map { NewTopic(it, 1, 1.toShort()) }
            if (toCreate.isNotEmpty()) {
                admin.createTopics(toCreate).all().get()
            }
        }
    }
}

class TestEventProducer(bootstrapServers: String) : AutoCloseable {

    private val producer = KafkaProducer<String, ByteArray>(Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)
        put(ProducerConfig.ACKS_CONFIG, "all")
        put(ProducerConfig.RETRIES_CONFIG, 3)
    })

    fun sendProduct(event: ProductEvent) {
        val bytes = JsonSerde.serialize(event)
        producer.send(ProducerRecord(Topics.PRODUCTS, event.productId, bytes)).get()
    }

    fun sendPrice(event: PriceEvent) {
        val bytes = JsonSerde.serialize(event)
        producer.send(ProducerRecord(Topics.PRICES, event.productId, bytes)).get()
    }

    fun sendStock(event: StockEvent) {
        val bytes = JsonSerde.serialize(event)
        producer.send(ProducerRecord(Topics.STOCK, event.productId, bytes)).get()
    }

    fun sendRawToTopic(topic: String, key: String, payload: ByteArray) {
        producer.send(ProducerRecord(topic, key, payload)).get()
    }

    override fun close() = producer.close()

    fun product(id: String, name: String, ts: Instant = Instant.now()) =
        ProductEvent(id, name, "Electronics", "Acme", null, ts, "c")

    fun price(id: String, amount: BigDecimal = BigDecimal("9.99"), ts: Instant = Instant.now()) =
        PriceEvent(id, amount, "USD", ts)

    fun stock(id: String, warehouse: String = "wh1", qty: Int = 100, ts: Instant = Instant.now()) =
        StockEvent(id, warehouse, qty, ts)
}
