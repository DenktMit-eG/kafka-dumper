package de.denktmit.kafka.utils

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.DescribeTopicsResult

fun AdminClient.getTopics(filter: (String) -> Boolean): DescribeTopicsResult {
    val topics = listTopics().listings().get()
        .filter { topicListing -> filter(topicListing.name()) }
        .map { it.name() }

    return describeTopics(topics)
}

