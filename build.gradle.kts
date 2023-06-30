import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.springframework.boot.gradle.tasks.bundling.BootBuildImage

plugins {
    id("org.springframework.boot") version "3.1.1"
    id("io.spring.dependency-management") version "1.1.0"
    kotlin("jvm") version "1.8.22"
    kotlin("plugin.spring") version "1.8.22"
}

apply(plugin = "io.spring.dependency-management")

group = "de.denktmit"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

extra["springShellVersion"] = "3.1.2"

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.springframework.shell:spring-shell-starter")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    implementation("io.projectreactor.kafka:reactor-kafka:1.3.18")
    implementation("org.apache.commons:commons-csv:1.10.0")
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.shell:spring-shell-dependencies:${property("springShellVersion")}")
    }
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs += "-Xjsr305=strict"
        jvmTarget = "17"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
tasks.named<BootBuildImage>("bootBuildImage") {
    docker {
        publishRegistry {
            username.set(System.getenv("GITHUB_ACTOR") ?: "")
            password.set(System.getenv("GITHUB_TOKEN") ?: "")
        }
    }
}