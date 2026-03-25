plugins {
    kotlin("jvm") version "2.3.20" apply false
}

val flinkVersion: String by project
val kotlinVersion: String by project

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")

    group = "com.pipeline"
    version = "1.0.0"

    repositories {
        mavenCentral()
        maven("https://packages.confluent.io/maven/")
    }

    configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
        jvmToolchain(17)
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}
