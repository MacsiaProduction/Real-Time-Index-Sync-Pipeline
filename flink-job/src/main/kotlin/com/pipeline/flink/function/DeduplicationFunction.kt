package com.pipeline.flink.function

import com.pipeline.model.EnrichedProduct
import com.pipeline.serde.JsonSerde
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.metrics.Gauge
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import java.time.Instant

/** Дедупликация: подавляет одинаковые документы (FNV-1a хеш). Замеряет latency. */
class DeduplicationFunction(private val ttlMs: Long) : KeyedProcessFunction<String, EnrichedProduct, EnrichedProduct>() {

    @Transient
    private lateinit var lastHashState: ValueState<Long>

    @Transient
    private lateinit var ttlTimerState: ValueState<Long>

    @Transient
    private lateinit var dedupFiltered: Counter

    private var lastLatencyMs: Long = 0L

    companion object {
        private val log = LoggerFactory.getLogger(DeduplicationFunction::class.java)
    }

    override fun open(parameters: Configuration) {
        lastHashState = runtimeContext.getState(
            ValueStateDescriptor("last-hash", Long::class.java)
        )
        ttlTimerState = runtimeContext.getState(
            ValueStateDescriptor("dedup-ttl-timer", Long::class.java)
        )
        dedupFiltered = runtimeContext.metricGroup.counter("dedup_filtered")
        runtimeContext.metricGroup.gauge("pipeline_latency_ms", Gauge<Long> { lastLatencyMs })
    }

    override fun processElement(
        doc: EnrichedProduct,
        ctx: Context,
        out: Collector<EnrichedProduct>
    ) {
        val hash = computeHash(doc)
        val lastHash = lastHashState.value()

        if (lastHash != null && lastHash == hash) {
            log.debug("Dedup: skipping unchanged document for {}", doc.productId)
            dedupFiltered.inc()
            return
        }

        // latency считаем по Kafka-событиям, не CDC
        val latestKafkaEventTs = listOfNotNull(doc.lastPriceUpdate, doc.lastStockUpdate).maxOrNull()
        if (latestKafkaEventTs != null) {
            lastLatencyMs = java.time.Duration.between(latestKafkaEventTs, Instant.now()).toMillis().coerceAtLeast(0L)
        }

        lastHashState.update(hash)
        registerTtlTimer(ctx)
        out.collect(doc)
    }

    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<EnrichedProduct>) {
        log.debug("Dedup TTL timer fired for key {}, clearing state", ctx.currentKey)
        lastHashState.clear()
        ttlTimerState.clear()
    }

    private fun registerTtlTimer(ctx: Context) {
        val existing = ttlTimerState.value()
        val newDeadline = ctx.timerService().currentProcessingTime() + ttlMs
        if (existing != null) {
            ctx.timerService().deleteProcessingTimeTimer(existing)
        }
        ctx.timerService().registerProcessingTimeTimer(newDeadline)
        ttlTimerState.update(newDeadline)
    }

    private fun computeHash(doc: EnrichedProduct): Long {
        // indexedAt исключён из хеша для идемпотентности
        val comparable = doc.copy(indexedAt = Instant.EPOCH)
        val bytes = JsonSerde.serialize(comparable)
        return fnv1a64(bytes)
    }

    private fun fnv1a64(data: ByteArray): Long {
        var hash = -3750763034362895579L  // FNV offset basis 64-bit
        for (b in data) {
            hash = hash xor (b.toLong() and 0xFF)
            hash *= 1099511628211L  // FNV prime 64-bit
        }
        return hash
    }
}
