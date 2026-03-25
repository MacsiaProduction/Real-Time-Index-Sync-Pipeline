package com.pipeline.flink.function

import com.pipeline.model.EnrichedProduct
import com.pipeline.model.PriceEvent
import com.pipeline.model.ProductEvent
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import java.time.Instant

/** Первый join: товары (CDC) × цены (Kafka). Идемпотентность по updatedAt, TTL-таймер 24ч. */
class ProductPriceJoinFunction(private val ttlMs: Long) : KeyedCoProcessFunction<String, ProductEvent, PriceEvent, EnrichedProduct>() {

    @Transient
    private lateinit var productState: ValueState<ProductEvent>

    @Transient
    private lateinit var priceState: ValueState<PriceEvent>

    @Transient
    private lateinit var ttlTimerState: ValueState<Long>

    @Transient
    private lateinit var productsProcessed: Counter

    @Transient
    private lateinit var pricesProcessed: Counter

    @Transient
    private lateinit var joinHits: Counter

    @Transient
    private lateinit var joinMisses: Counter

    companion object {
        private val log = LoggerFactory.getLogger(ProductPriceJoinFunction::class.java)
    }

    override fun open(parameters: Configuration) {
        productState = runtimeContext.getState(
            ValueStateDescriptor("product", ProductEvent::class.java)
        )
        priceState = runtimeContext.getState(
            ValueStateDescriptor("price", PriceEvent::class.java)
        )
        ttlTimerState = runtimeContext.getState(
            ValueStateDescriptor("ttl-timer", Long::class.java)
        )
        val mg = runtimeContext.metricGroup
        productsProcessed = mg.counter("events_processed_products")
        pricesProcessed = mg.counter("events_processed_prices")
        joinHits = mg.counter("join_hits")
        joinMisses = mg.counter("join_misses")
    }

    override fun processElement1(
        product: ProductEvent,
        ctx: Context,
        out: Collector<EnrichedProduct>
    ) {
        if (product.isDelete) {
            log.debug("Product {} deleted, clearing state", product.productId)
            productState.clear()
            priceState.clear()
            cancelTtlTimer(ctx)
            return
        }

        val existing = productState.value()
        // идемпотентность: отбросить устаревшие
        if (existing != null && !product.updatedAt.isAfter(existing.updatedAt)) {
            log.debug("Ignoring stale product event for {} (event={}, stored={})",
                product.productId, product.updatedAt, existing.updatedAt)
            return
        }

        productState.update(product)
        productsProcessed.inc()
        if (priceState.value() != null) joinHits.inc() else joinMisses.inc()
        registerTtlTimer(ctx)
        out.collect(buildEnriched(product.productId))
    }

    override fun processElement2(
        price: PriceEvent,
        ctx: Context,
        out: Collector<EnrichedProduct>
    ) {
        val existing = priceState.value()
        if (existing != null && !price.updatedAt.isAfter(existing.updatedAt)) {
            log.debug("Ignoring stale price event for {} (event={}, stored={})",
                price.productId, price.updatedAt, existing.updatedAt)
            return
        }

        priceState.update(price)
        pricesProcessed.inc()
        if (productState.value() != null) joinHits.inc() else joinMisses.inc()

        registerTtlTimer(ctx)
        out.collect(buildEnriched(price.productId))
    }

    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<EnrichedProduct>) {
        log.debug("TTL timer fired for key {}, clearing state", ctx.currentKey)
        productState.clear()
        priceState.clear()
        ttlTimerState.clear()
    }

    private fun buildEnriched(productId: String): EnrichedProduct {
        val product = productState.value()
        val price = priceState.value()
        return EnrichedProduct(
            productId = productId,
            name = product?.name,
            category = product?.category,
            brand = product?.brand,
            description = product?.description,
            price = price?.price,
            currency = price?.currency,
            totalStock = null,  // заполнит следующий join
            stockByWarehouse = emptyMap(),
            lastProductUpdate = product?.updatedAt,
            lastPriceUpdate = price?.updatedAt,
            lastStockUpdate = null,
            completeness = EnrichedProduct.computeCompleteness(product?.name, price?.price, null),
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

    private fun cancelTtlTimer(ctx: Context) {
        val existing = ttlTimerState.value() ?: return
        ctx.timerService().deleteProcessingTimeTimer(existing)
        ttlTimerState.clear()
    }
}
