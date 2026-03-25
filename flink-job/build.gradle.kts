plugins {
    id("com.gradleup.shadow") version "8.3.6"
}

val flinkVersion: String by project
val flinkKafkaConnectorVersion: String by project
val flinkEsConnectorVersion: String by project
val flinkCdcVersion: String by project
val jacksonVersion: String by project
val log4jVersion: String by project
val junitVersion: String by project

dependencies {
    implementation(project(":common"))

    // Flink core (provided)
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")
    compileOnly("org.apache.flink:flink-runtime:$flinkVersion")

    // Kafka/ES коннекторы (provided)
    compileOnly("org.apache.flink:flink-connector-kafka:$flinkKafkaConnectorVersion")
    compileOnly("org.apache.flink:flink-connector-elasticsearch8:$flinkEsConnectorVersion")

    // CDC + Debezium — bundled в shadow JAR из-за конфликта classloader
    implementation("org.apache.flink:flink-connector-postgres-cdc:$flinkCdcVersion")

    // Jackson (bundled)
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    // Logging
    implementation("org.apache.logging.log4j:log4j-api:$log4jVersion")
    implementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")

    // Test
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-clients:$flinkVersion")
    testImplementation("org.apache.flink:flink-runtime:$flinkVersion")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.apache.flink:flink-connector-kafka:$flinkKafkaConnectorVersion")
    testImplementation("org.apache.flink:flink-connector-elasticsearch8:$flinkEsConnectorVersion")
    testImplementation("org.apache.flink:flink-connector-postgres-cdc:$flinkCdcVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Коннекторы для Docker — не в shadow JAR, а в /opt/flink/lib/
val flinkConnectors by configurations.creating {
    extendsFrom(configurations.compileOnly.get())
    isTransitive = true
}

val copyFlinkConnectors by tasks.registering(Copy::class) {
    description = "Copy Flink connector JARs and their dependencies to build/connectors for Docker"
    from(flinkConnectors.resolvedConfiguration.resolvedArtifacts
        .filter { artifact ->
            val id = artifact.moduleVersion.id
            id.group != "org.apache.flink" ||
            id.module.name.startsWith("flink-connector-")
        }
        .map { it.file })
    into(layout.buildDirectory.dir("connectors"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.shadowJar {
    archiveBaseName.set("rt-index-pipeline")
    archiveClassifier.set("")
    archiveVersion.set("")
    mergeServiceFiles()
    dependsOn(copyFlinkConnectors)

    dependencies {
        exclude(dependency("org.apache.flink:flink-streaming-java:"))
        exclude(dependency("org.apache.flink:flink-clients:"))
        exclude(dependency("org.apache.flink:flink-runtime:"))
        exclude(dependency("org.apache.flink:flink-connector-kafka:"))
        exclude(dependency("org.apache.flink:flink-connector-elasticsearch8:"))
        exclude(dependency("org.apache.flink:flink-connector-base:"))
        // kafka-clients provided; Debezium Kafka Connect bundled
        exclude(dependency("org.apache.kafka:kafka-clients:"))
    }

    // relocate Jackson (конфликт с Flink)
    relocate("com.fasterxml.jackson", "com.pipeline.shaded.jackson")
}

tasks.test {
    jvmArgs(
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.time=ALL-UNNAMED",
        "--add-opens=java.rmi/sun.rmi.transport=ALL-UNNAMED"
    )
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
