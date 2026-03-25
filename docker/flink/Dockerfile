FROM flink:1.20.3-java17

# Add Flink Prometheus metrics reporter
RUN wget -q -O /opt/flink/lib/flink-metrics-prometheus-1.20.3.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.20.3/flink-metrics-prometheus-1.20.3.jar

# Copy the pipeline fat JAR
COPY flink-job/build/libs/rt-index-pipeline.jar /opt/flink/usrlib/rt-index-pipeline.jar

# Copy connector JARs and their dependencies (kafka-clients, ES client, etc.)
COPY flink-job/build/connectors/ /opt/flink/lib/

# Copy pipeline configuration
COPY flink-job/src/main/resources/pipeline.properties /opt/flink/conf/pipeline.properties
COPY flink-job/src/main/resources/log4j2.properties /opt/flink/conf/log4j-console.properties

# Flink metrics reporter configuration (appended to flink-conf.yaml)
RUN echo "" >> /opt/flink/conf/config.yaml && \
    echo "metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory" >> /opt/flink/conf/config.yaml && \
    echo "metrics.reporter.prom.port: 9249" >> /opt/flink/conf/config.yaml
