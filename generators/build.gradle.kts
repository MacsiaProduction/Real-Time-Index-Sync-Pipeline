plugins {
    id("com.gradleup.shadow") version "8.3.6"
    application
}

val jacksonVersion: String by project
val kafkaVersion: String by project
val postgresDriverVersion: String by project
val coroutinesVersion: String by project
val log4jVersion: String by project

dependencies {
    implementation(project(":common"))
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    implementation("org.postgresql:postgresql:$postgresDriverVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")
    implementation("org.apache.logging.log4j:log4j-api:$log4jVersion")
    implementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
}

application {
    mainClass.set("com.pipeline.generators.GeneratorMainKt")
}

tasks.shadowJar {
    archiveBaseName.set("generators")
    archiveClassifier.set("")
    archiveVersion.set("")
    mergeServiceFiles()
    manifest {
        attributes["Main-Class"] = "com.pipeline.generators.GeneratorMainKt"
    }
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
