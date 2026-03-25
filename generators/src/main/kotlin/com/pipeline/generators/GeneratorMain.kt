package com.pipeline.generators

import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("GeneratorMain")

fun main() = runBlocking {
    val config = GeneratorConfig()
    log.info("Starting generators: pool={} pricesRate={}/s stockRate={}/s",
        config.productPoolSize, config.pricesRatePerSec, config.stockRatePerSec)

    val productGen = ProductGenerator(config)
    val priceGen   = PriceGenerator(config)
    val stockGen   = StockGenerator(config)

    Runtime.getRuntime().addShutdownHook(Thread {
        productGen.close()
        priceGen.close()
        stockGen.close()
        log.info("Generators shut down")
    })

    val productJob = async { productGen.run() }
    val priceJob   = async { priceGen.run() }
    val stockJob   = async { stockGen.run() }

    productJob.await()
    priceJob.await()
    stockJob.await()
}
