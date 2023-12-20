plugins {
    val kotlinVersion = "1.7.0"

    kotlin("jvm") version kotlinVersion
    kotlin("plugin.spring") version kotlinVersion apply false
    id("org.springframework.boot") version "2.7.1" apply false
    id("io.spring.dependency-management") version "1.0.11.RELEASE"
    id("maven-publish")
}

group = "com.github.3jin-p"
version = "v0.0.1-alpha.8"

repositories {
    mavenCentral()
    maven { setUrl("https://jitpack.io") }
}

apply(plugin = "io.spring.dependency-management")

dependencyManagement {
    imports {
        mavenBom(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES)
    }
}

val AWS_SDK = "2.20.42"

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-batch")
    implementation("software.amazon.awssdk:athena")
    implementation(platform("software.amazon.awssdk:bom:${AWS_SDK}"))
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:sts")
    implementation("software.amazon.awssdk:aws-core")
    implementation("software.amazon.awssdk:ssm")
    implementation("software.amazon.awssdk:sso")
    implementation("software.amazon.awssdk:quicksight")
    implementation(kotlin("stdlib-jdk8"))
}

tasks.withType<Test> {
    useJUnitPlatform()
}


