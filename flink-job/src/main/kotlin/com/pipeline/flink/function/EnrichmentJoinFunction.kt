package com.pipeline.flink.function

import com.pipeline.model.EnrichedProduct
import com.pipeline.model.StockEvent
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import java.time.Instant

/** Второй join: объединяет частичный документ (stage 1) с данными остатков. */
class EnrichmentJoinFunction(private val ttlMs: Long) : KeyedCoProcessFunction<String, EnrichedProduct, StockEvent, EnrichedProduct>() {

    @Transient
    private lateinit var partialDocState: ValueState<EnrichedProduct>

    @Transient
    private lateinit var stockState: MapState<String, Int>

    @Transient
    private lateinit var ttlTimerState: ValueState<Long>

    @Transient
    private lateinit var lastStockTsState: ValueState<Long>

    @Transient
    private lateinit var stockProcessed: Counter

    companion object {
        private val log = LoggerFactory.getLogger(EnrichmentJoinFunction::class.java)
    }

    override fun open(parameters: Configuration) {
        partialDocState = runtimeContext.getState(
            ValueStateDescriptor("partial-doc", EnrichedProduct::class.java)
        )
        stockState = runtimeContext.getMapState(
            MapStateDescriptor("stock-by-warehouse", String::class.java, Int::class.java)
        )
        ttlTimerState = runtimeContext.getState(
            ValueStateDescriptor("ttl-timer", Long::class.java)
        )
        lastStockTsState = runtimeContext.getState(
            ValueStateDescriptor("last-stock-ts", Long::class.java)
        )
        stockProcessed = runtimeContext.metricGroup.counter("events_processed_stock")
    }

    override fun processElement1(
        partial: EnrichedProduct,
        ctx: Context,
        out: Collector<EnrichedProduct>
    ) {
        partialDocState.update(partial)
        registerTtlTimer(ctx)
        out.collect(buildFull(partial.productId))
    }

    override fun processElement2(
        stock: StockEvent,
        ctx: Context,
        out: Collector<EnrichedProduct>
    ) {
        stockProcessed.inc()
        stockState.put(stock.warehouseId, stock.quantity)

        // максимальный timestamp остатков
        val currentMaxTs = lastStockTsState.value() ?: 0L
        val newTs = stock.updatedAt.toEpochMilli()
        if (newTs > currentMaxTs) {
            lastStockTsState.update(newTs)
        }

        registerTtlTimer(ctx)
        out.collect(buildFull(stock.productId))
    }

    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<EnrichedProduct>) {
        log.debug("TTL timer fired for key {}, clearing state", ctx.currentKey)
        partialDocState.clear()
        stockState.clear()
        ttlTimerState.clear()
        lastStockTsState.clear()
    }

    private fun buildFull(productId: String): EnrichedProduct {
        val partial = partialDocState.value()
        val warehouseMap: Map<String, Int> = stockState.entries()?.associate { it.key to it.value } ?: emptyMap()
        val totalStock = if (warehouseMap.isEmpty()) null else warehouseMap.values.sum()
        val lastStockUpdate = lastStockTsState.value()?.let { Instant.ofEpochMilli(it) }

        return EnrichedProduct(
            productId = productId,
            name = partial?.name,
            category = partial?.category,
            brand = partial?.brand,
            description = partial?.description,
            price = partial?.price,
            currency = partial?.currency,
            totalStock = totalStock,
            stockByWarehouse = warehouseMap,
            lastProductUpdate = partial?.lastProductUpdate,
            lastPriceUpdate = partial?.lastPriceUpdate,
            lastStockUpdate = lastStockUpdate,
            completeness = EnrichedProduct.computeCompleteness(partial?.name, partial?.price, totalStock),
            indexedAt = Instant.now()
        )
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
}
