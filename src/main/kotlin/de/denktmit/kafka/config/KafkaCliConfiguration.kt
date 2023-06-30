package de.denktmit.kafka.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "application")
data class KafkaCliConfiguration(
    val backupTopics: List<String> = emptyList(),
    val restoreTopics: Map<String, String> = emptyMap()
)