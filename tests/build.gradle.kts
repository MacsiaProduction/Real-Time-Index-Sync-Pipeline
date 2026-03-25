val flinkVersion: String by project
val flinkKafkaConnectorVersion: String by project
val flinkEsConnectorVersion: String by project
val flinkCdcVersion: String by project
val jacksonVersion: String by project
val testcontainersVersion: String by project
val junitVersion: String by project
val elasticsearchClientVersion: String by project
val postgresDriverVersion: String by project
val kafkaVersion: String by project

dependencies {
    implementation(project(":common"))

    // Flink
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.flink:flink-runtime:$flinkVersion")
    implementation("org.apache.flink:flink-test-utils:$flinkVersion")
    implementation("org.apache.flink:flink-connector-kafka:$flinkKafkaConnectorVersion")
    implementation("org.apache.flink:flink-connector-elasticsearch8:$flinkEsConnectorVersion")
    implementation("org.apache.flink:flink-connector-postgres-cdc:$flinkCdcVersion")

    implementation(project(":flink-job"))

    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")

    implementation("co.elastic.clients:elasticsearch-java:$elasticsearchClientVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    implementation("org.postgresql:postgresql:$postgresDriverVersion")

    // Test
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // Testcontainers
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:kafka:$testcontainersVersion")
    testImplementation("org.testcontainers:elasticsearch:$testcontainersVersion")
    testImplementation("org.testcontainers:postgresql:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
}

configurations.all {
    resolutionStrategy.force("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
}
