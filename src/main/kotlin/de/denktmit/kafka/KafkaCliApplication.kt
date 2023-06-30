package de.denktmit.kafka

import de.denktmit.kafka.config.KafkaCliConfiguration
import org.springframework.boot.Banner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties


@SpringBootApplication
@EnableConfigurationProperties(KafkaCliConfiguration::class)
class KafkaCliApplication

fun main(args: Array<String>) {
    SpringApplication(KafkaCliApplication::class.java).apply {
        setBannerMode(Banner.Mode.OFF)
    }.run(*args)
}
