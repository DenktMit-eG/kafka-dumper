package de.denktmit.kafka.command

import de.denktmit.kafka.config.CsvConfig.Companion.CSV_FORMAT
import de.denktmit.kafka.config.KafkaCliConfiguration
import de.denktmit.kafka.utils.*
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListOffsetsResult
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import reactor.core.publisher.Flux
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong


@ShellComponent
class KafkaTopicBackupCommand(
    var receiverOptions: ReceiverOptions<ByteBuffer, ByteBuffer>,
    var adminClient: AdminClient,
    val config: KafkaCliConfiguration
) {
    companion object {
        val LOGGER: Logger = LoggerFactory.getLogger(KafkaTopicBackupCommand::class.java)
    }

    fun discoverTopics(topics: List<String>) =
        Flux.fromIterable(adminClient.describeTopics(topics).topicNameValues().values)
            .flatMap { value ->
                Flux.fromIterable(
                    adminClient.listOffsets(
                        value.get().partitions()
                            .associate { TopicPartition(value.get().name(), it.partition()) to OffsetSpec.latest() })
                        .all()
                        .get().entries
                )
            }

    fun consumeTopics(it: MutableMap.MutableEntry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>): Flux<*> {
        val path = Path.of("${it.key.topic()}-${it.key.partition()}.csv")
        val delta = AtomicLong(-1L)

        // offsets are 0 based, so we need to subtract 1
        val offset = it.value.offset() - 1

        val opts = receiverOptions.assignment(listOf(it.key))
            .addAssignListener { onAssign -> onAssign.forEach { assign -> assign.seekToBeginning(); } }

        val topic = it.key.topic()
        val part = it.key.partition()

        return ReactiveKafkaConsumerTemplate(opts)
            .receive()
            .logEveryNthObservable(
                { rn, r -> LOGGER.info("Got {} msgs in {}-{} [{} / {}]", rn, topic, part, r.offset(), offset) },
                { rn -> LOGGER.info("Got {} msgs in total for {}-{} with offset {} ", rn, topic, part, offset) },
                10_000
            )
            .handle<ReceiverRecord<ByteBuffer, ByteBuffer>?> { r, sink ->
                sink.next(r)
                if (r.offset() == offset) {
                    sink.complete()
                    LOGGER.info("Completed ${it.key.topic()}-${it.key.partition()} with offset $offset")
                }
            }.map {
                val d = delta.get()
                delta.set(it.timestamp())

                val deltaOffset = if (d == -1L) {
                    0
                } else {
                    d - it.offset()
                }

                // DELTA, TIMESTAMP, OFFSET, KEY, MESSAGE
                arrayOf<Any>(deltaOffset, it.timestamp(), it.offset(), it.b64Key(), it.b64Value())
            }
            .transform { influx -> path.writeCSV(CSV_FORMAT, influx) }
    }

    @ShellMethod(key = ["backup"], value = "Backup a topic")
    fun backupTopic(topics: List<String>) {
        val countDownLatch = CountDownLatch(1)

        discoverTopics(topics).flatMap(::consumeTopics).doAfterTerminate { countDownLatch.countDown() }.subscribe()

        countDownLatch.await()
    }

    @ShellMethod(key = ["backup-config"], value = "Backup a topic")
    fun backupTopic() {
        val countDownLatch = CountDownLatch(1)

        config.backupTopics.forEach {
            LOGGER.info("Backing up topics $it")
        }

        discoverTopics(config.backupTopics).flatMap(::consumeTopics).doAfterTerminate { countDownLatch.countDown() }
            .subscribe()

        countDownLatch.await()
    }
}
