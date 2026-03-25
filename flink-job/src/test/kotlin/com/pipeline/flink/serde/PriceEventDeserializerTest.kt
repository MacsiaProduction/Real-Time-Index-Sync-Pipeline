package com.pipeline.flink.serde

import com.pipeline.flink.TestInitializationContext
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant

class PriceEventDeserializerTest {

    private lateinit var deserializer: PriceEventDeserializer

    @BeforeEach
    fun setup() {
        deserializer = PriceEventDeserializer()
        deserializer.open(TestInitializationContext())
    }

    private fun priceJson(
        productId: String = "p1",
        price: String = "19.99",
        currency: String = "USD",
        updatedAt: String = Instant.now().toString()
    ) = """{"productId":"$productId","price":$price,"currency":"$currency","updatedAt":"$updatedAt"}"""

    @Test
    fun `valid price event deserializes correctly`() {
        val result = deserializer.deserialize(priceJson().toByteArray())

        assertNotNull(result)
        assertEquals("p1", result!!.productId)
        assertEquals("19.99", result.price.toPlainString())
        assertEquals("USD", result.currency)
    }

    @Test
    fun `different currencies deserialize`() {
        val result = deserializer.deserialize(priceJson(currency = "EUR", price = "25.00").toByteArray())

        assertNotNull(result)
        assertEquals("EUR", result!!.currency)
    }

    @Test
    fun `blank product id returns null`() {
        val result = deserializer.deserialize(priceJson(productId = "").toByteArray())
        assertNull(result)
    }

    @Test
    fun `whitespace-only product id returns null`() {
        val result = deserializer.deserialize(priceJson(productId = "   ").toByteArray())
        assertNull(result)
    }

    @Test
    fun `malformed json returns null`() {
        val result = deserializer.deserialize("not valid json {{{".toByteArray())
        assertNull(result)
    }

    @Test
    fun `empty bytes returns null`() {
        val result = deserializer.deserialize(ByteArray(0))
        assertNull(result)
    }

    @Test
    fun `future timestamp beyond tolerance returns null`() {
        val futureTs = Instant.now().plusSeconds(120)
        val result = deserializer.deserialize(priceJson(updatedAt = futureTs.toString()).toByteArray())
        assertNull(result)
    }

    @Test
    fun `timestamp just within 1-minute tolerance is accepted`() {
        val nearFutureTs = Instant.now().plusSeconds(30)
        val result = deserializer.deserialize(priceJson(updatedAt = nearFutureTs.toString()).toByteArray())
        assertNotNull(result)
    }

    @Test
    fun `missing required price field returns null`() {
        val json = """{"productId":"p1","currency":"USD","updatedAt":"${Instant.now()}"}"""
        val result = deserializer.deserialize(json.toByteArray())
        assertNull(result)
    }

    @Test
    fun `high precision price is preserved`() {
        val result = deserializer.deserialize(priceJson(price = "1234567.89").toByteArray())

        assertNotNull(result)
        assertEquals("1234567.89", result!!.price.toPlainString())
    }

    @Test
    fun `zero price is valid`() {
        val result = deserializer.deserialize(priceJson(price = "0.00").toByteArray())
        assertNotNull(result)
        assertTrue(result!!.price.compareTo(java.math.BigDecimal.ZERO) == 0)
    }
}
