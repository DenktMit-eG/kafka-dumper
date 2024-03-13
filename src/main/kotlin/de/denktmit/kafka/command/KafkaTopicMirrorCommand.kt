package de.denktmit.kafka.command

import com.google.protobuf.DynamicMessage
import com.google.protobuf.util.JsonFormat
import de.denktmit.kafka.config.KafkaCliConfiguration
import de.denktmit.kafka.utils.getTopics
import de.denktmit.kafka.utils.logEveryNthObservable
import de.denktmit.kafka.utils.logThroughputEveryDuration
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.jline.terminal.Terminal
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.shell.standard.AbstractShellComponent
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.util.retry.Retry
import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.CountDownLatch


@ShellComponent
class KafkaTopicMirrorCommand(
    var receiverOptions: ReceiverOptions<ByteBuffer, ByteBuffer>,
    val senderOptions: SenderOptions<ByteBuffer, ByteBuffer>,
    val kafkaProperties: KafkaProperties,
    val config: KafkaCliConfiguration,
    val mirrorConfig: MirrorConfig,
) : AbstractShellComponent() {
    companion object {
        val LOGGER: Logger = LoggerFactory.getLogger(KafkaTopicMirrorCommand::class.java)
    }

    val consumerAdminClient: AdminClient = AdminClient.create(kafkaProperties.buildConsumerProperties(null))
    val producerAdminClient: AdminClient = AdminClient.create(kafkaProperties.buildProducerProperties(null))

    val countDownLatch = CountDownLatch(1)
    val scheduler = Schedulers.boundedElastic()

    fun subscribe(flux: Flux<*>) {
        val sub = flux.doFinally { countDownLatch.countDown() }.subscribe()

        terminal.handle(Terminal.Signal.INT) { sub.dispose() }

        countDownLatch.await()
        scheduler.dispose()
    }

    @ShellMethod(key = ["mirror-json"], value = "unidirectional topic mirroring")
    fun mirrorJson() {
        val printer = JsonFormat.printer()

        val deserializer = KafkaProtobufDeserializer<DynamicMessage>(schemaRegistry())

        mirror {
            val rawMsg = it.value()
            rawMsg.position(0)
            val msgArray = ByteArray(rawMsg.remaining())
            rawMsg.get(msgArray)

            val msg = printer.print(deserializer.deserialize(it.topic(), msgArray)).toByteArray()

            SenderRecord.create(
                ProducerRecord(
                    it.topic(),
                    it.partition(),
                    it.timestamp(),
                    it.key(),
                    ByteBuffer.wrap(msg),
                    it.headers()
                ), null
            )
        }
    }

    private fun schemaRegistry(): SchemaRegistryClient? {
        val configs = kafkaProperties.buildAdminProperties(null)

        val urlString = configs["schema.registry.url"] as String
        val urls = urlString.split(",".toRegex()).dropLastWhile { it.isEmpty() }

        return SchemaRegistryClientFactory.newClient(
            urls,
            100_000,
            listOf(ProtobufSchemaProvider()),
            configs,
            emptyMap()
        )
    }

    @ShellMethod(key = ["mirror"], value = "unidirectional topic mirroring")
    fun mirrorRaw() {
        mirror {
            SenderRecord.create(
                ProducerRecord(it.topic(), it.partition(), it.timestamp(), it.key(), it.value(), it.headers()), null
            )
        }
    }

    fun mirror(converter: (ConsumerRecord<ByteBuffer, ByteBuffer>) -> SenderRecord<ByteBuffer, ByteBuffer, Nothing?>?) {
        val flux = Flux.fromIterable(replicateTopicConfigs()).flatMap { partitions ->
            val opts = receiverOptions.assignment(partitions)
//                .addAssignListener { onAssign -> onAssign.forEach { assign -> assign.seekToBeginning(); } }

            val topicName = partitions[0].topic()

            val inFlux = ReactiveKafkaConsumerTemplate(opts).receiveAutoAck().subscribeOn(scheduler)
                .retryWhen(
                    Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(10))
                        .filter { throwable -> throwable is DisconnectException }
                )
                .onErrorResume(TopicAuthorizationException::class.java) { ex: TopicAuthorizationException ->
                    LOGGER.error("Unable to consume topic", ex)
                    Flux.empty()
                }
                .map(converter)
                .doFinally { countDownLatch.countDown() }
                .logEveryNthObservable(
                    { rn, _ -> LOGGER.info("[{}] consumed {} msgs ", topicName, rn) },
                    { rn -> LOGGER.info("[{}] consumed {} msgs in total ", topicName, rn) },
                    10_000
                )

            KafkaSender.create(senderOptions).send(inFlux)
                .onErrorResume(TopicAuthorizationException::class.java) { ex: TopicAuthorizationException ->
                    LOGGER.error("Unable to produce to topic", ex)
                    Flux.empty()
                }
                .logEveryNthObservable(
                    { rn, _ -> LOGGER.info("[{}] produced {} msgs ", topicName, rn) },
                    { rn -> LOGGER.info("[{}] produced {} msgs in total ", topicName, rn) },
                    10_000
                )
        }.logThroughputEveryDuration { throughput, totalMessages ->
            LOGGER.info("Current throughput: $throughput items/second, Total messages: $totalMessages")
        }

        subscribe(flux)
    }

    private fun topicFilter(): (String) -> Boolean {
        if (mirrorConfig.topics.isNotEmpty()) {
            return fun(topicName: String): Boolean = mirrorConfig.topics.contains(topicName)
        }

        val allowedTopicPattern = Regex(mirrorConfig.allowedTopicPattern)
        return fun(topicName: String): Boolean = allowedTopicPattern.matches(topicName)
    }

    fun replicateTopicConfigs(): List<List<TopicPartition>> {
        val allowed = Regex(mirrorConfig.allowedConfigrationPattern)
        val denied = mirrorConfig.deniedConfigrationPattern?.let { Regex(it) }

        val sourceTopics = consumerAdminClient.getTopics(topicFilter()).allTopicNames().get()
        val targetTopics = producerAdminClient.getTopics(topicFilter()).allTopicNames().get().keys

        val topicConfiguration = sourceTopics.filter {
            !targetTopics.contains(it.key)
        }.map { topicName -> ConfigResource(ConfigResource.Type.TOPIC, topicName.key) }

        val describeConfigResult = consumerAdminClient.describeConfigs(topicConfiguration).all().get()

        val newTopics = describeConfigResult.map { (key, description) ->
            val topicName = key.name()
            val partitions = sourceTopics[topicName]!!.partitions()

            val config = description.entries()
                .filter { allowed.matches(it.name()) && (denied?.matches(it.name())?.not() ?: true) }
                .associate { it.name() to it.value() }

            NewTopic(
                topicName,
                partitions.size,
                partitions[0].replicas().size.toShort()
            ).configs(config)
        }

        producerAdminClient.createTopics(newTopics).all().get()

        return sourceTopics.values.map { topic ->
            LOGGER.info(
                "Subscribe to topic {} with partitions [{}]",
                topic.name(),
                topic.partitions().map { it.partition() }
            )

            topic.partitions().map {
                TopicPartition(topic.name(), it.partition())
            }
        }
    }

}

@ConfigurationProperties(prefix = "mirror")
data class MirrorConfig(
    val topics: List<String> = emptyList(),
    val allowedTopicPattern: String = ".*",
    val allowedConfigrationPattern: String = ".*",
    val deniedConfigrationPattern: String? = "",
)
