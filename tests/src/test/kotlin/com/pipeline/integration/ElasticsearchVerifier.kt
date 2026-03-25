package com.pipeline.integration

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import java.math.BigDecimal
import java.time.Instant

data class EsDoc(
    val productId: String,
    val name: String?,
    val price: BigDecimal?,
    val totalStock: Int?,
    val completeness: Double,
    val indexedAt: Instant?,
    val lastProductUpdate: Instant?,
    val lastPriceUpdate: Instant?,
    val lastStockUpdate: Instant?
)

class ElasticsearchVerifier(esHost: String, esPort: Int) : AutoCloseable {

    private val mapper: ObjectMapper = ObjectMapper().apply {
        registerModule(KotlinModule.Builder().build())
        registerModule(JavaTimeModule())
    }

    private val restClient: RestClient = RestClient.builder(HttpHost(esHost, esPort)).build()
    private val transport = RestClientTransport(restClient, JacksonJsonpMapper(mapper))
    private val client = ElasticsearchClient(transport)

    fun ensureIndex(indexName: String) {
        val exists = client.indices().exists { it.index(indexName) }.value()
        if (!exists) {
            client.indices().create { it.index(indexName) }
        }
    }

    fun awaitDocument(indexName: String, productId: String, timeoutMs: Long = 30_000L): EsDoc {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            val doc = getDocument(indexName, productId)
            if (doc != null) return doc
            Thread.sleep(500)
        }
        throw AssertionError("Document $productId not found in index '$indexName' within ${timeoutMs}ms")
    }

    fun awaitCompleteness(
        indexName: String,
        productId: String,
        minCompleteness: Double,
        timeoutMs: Long = 30_000L
    ): EsDoc {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            val doc = getDocument(indexName, productId)
            if (doc != null && doc.completeness >= minCompleteness) return doc
            Thread.sleep(500)
        }
        val last = getDocument(indexName, productId)
        throw AssertionError(
            "Document $productId completeness < $minCompleteness within ${timeoutMs}ms " +
            "(actual: ${last?.completeness})"
        )
    }

    fun awaitDocumentCount(indexName: String, count: Int, timeoutMs: Long = 60_000L): Int {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            val current = countDocuments(indexName)
            if (current >= count) return current
            Thread.sleep(1_000)
        }
        val current = countDocuments(indexName)
        throw AssertionError("Expected $count docs in '$indexName' within ${timeoutMs}ms, got $current")
    }

    fun getDocument(indexName: String, productId: String): EsDoc? {
        return try {
            val response = client.get({ it.index(indexName).id(productId) }, JsonNode::class.java)
            if (!response.found()) return null
            val src = response.source() ?: return null
            EsDoc(
                productId  = productId,
                name       = src.get("name")?.asText(),
                price      = src.get("price")?.let { n -> if (n.isNull) null else BigDecimal(n.asText()) },
                totalStock = src.get("totalStock")?.let { n -> if (n.isNull) null else n.asInt() },
                completeness = src.get("completeness")?.asDouble() ?: 0.0,
                indexedAt = src.get("indexedAt")?.takeIf { !it.isNull }?.asText()?.let { Instant.parse(it) },
                lastProductUpdate = src.get("lastProductUpdate")?.takeIf { !it.isNull }?.asText()?.let { Instant.parse(it) },
                lastPriceUpdate = src.get("lastPriceUpdate")?.takeIf { !it.isNull }?.asText()?.let { Instant.parse(it) },
                lastStockUpdate = src.get("lastStockUpdate")?.takeIf { !it.isNull }?.asText()?.let { Instant.parse(it) }
            )
        } catch (_: Exception) {
            null
        }
    }

    fun countDocuments(indexName: String): Int {
        return try {
            val response = client.search({ r ->
                r.index(indexName).query(Query.of { q -> q.matchAll { it } })
                    .size(0)
            }, JsonNode::class.java)
            response.hits().total()?.value()?.toInt() ?: 0
        } catch (_: Exception) {
            0
        }
    }

    override fun close() {
        runCatching { transport.close() }
        runCatching { restClient.close() }
    }
}
