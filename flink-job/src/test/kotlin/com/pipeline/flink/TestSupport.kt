package com.pipeline.flink

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup
import org.apache.flink.util.UserCodeClassLoader

class TestInitializationContext : DeserializationSchema.InitializationContext {
    override fun getMetricGroup(): MetricGroup = UnregisteredMetricsGroup()
    override fun getUserCodeClassLoader(): UserCodeClassLoader = object : UserCodeClassLoader {
        override fun asClassLoader(): ClassLoader = Thread.currentThread().contextClassLoader
        override fun registerReleaseHookIfAbsent(releaseHookName: String, releaseHook: Runnable) {}
    }
}
