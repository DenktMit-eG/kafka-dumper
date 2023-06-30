package de.denktmit.kafka.command

import de.denktmit.kafka.config.CsvConfig.Companion.CSV_FORMAT
import de.denktmit.kafka.config.CsvConfig.Companion.KEY
import de.denktmit.kafka.config.CsvConfig.Companion.MESSAGE
import de.denktmit.kafka.config.CsvConfig.Companion.TIMESTAMP
import de.denktmit.kafka.config.KafkaCliConfiguration
import de.denktmit.kafka.utils.base64Encoded
import de.denktmit.kafka.utils.base64EncodedBuf
import de.denktmit.kafka.utils.csvRead
import de.denktmit.kafka.utils.logEveryNthObservable
import org.apache.commons.csv.CSVRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import reactor.core.publisher.Flux
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import kotlin.io.path.listDirectoryEntries


@ShellComponent
class KafkaTopicRestoreCommand(
    val senderOptions: SenderOptions<ByteBuffer, ByteBuffer>,
    val config: KafkaCliConfiguration
) {
    companion object {
        val LOGGER: Logger = LoggerFactory.getLogger(KafkaTopicRestoreCommand::class.java)
        val TOPIC_REGEX = "^([a-z0-9.\\-_]+)-(\\d+)\\.csv$".toRegex()
    }

    @ShellMethod(key = ["restore"], value = "Restore a topic")
    fun restoreTopics(path: String) {
        val countDownLatch = CountDownLatch(1)

        val resolvedCsvFiles =
            Path.of(path).listDirectoryEntries("*.csv").filter { it.fileName.toString().matches(TOPIC_REGEX) }.map {
                val (topic, partition) = TOPIC_REGEX.find(it.fileName.toString())!!.destructured
                TopicLoadFile(topic, partition.toInt(), it)
            }

        Flux.fromIterable(resolvedCsvFiles).flatMap { csv ->
            val newSchemaId = config.restoreTopics[csv.topic]?.let { schemaId ->
                ByteBuffer.allocate(Int.SIZE_BYTES).putInt(schemaId).array()
            } ?: error("No schema id found for topic ${csv.topic}")

            KafkaSender.create(senderOptions).send(csv.path.csvRead(CSV_FORMAT)
                .map { toRecord(it, csv.topic, csv.partition, newSchemaId) })
                .doOnError { e -> LOGGER.error("Send failed", e) }
                .logEveryNthObservable(
                    { rn, _ -> LOGGER.info("Messages {} send to {}-{}", rn, csv.topic, csv.partition) },
                    { rn -> LOGGER.info("Messages {} in total send to {}-{}", rn, csv.topic, csv.partition) },
                    10_000
                )
        }.doAfterTerminate { countDownLatch.countDown() }.subscribe()

        countDownLatch.await()
    }

    private fun toRecord(
        csvRecord: CSVRecord,
        topic: String,
        partition: Int,
        schemaId: ByteArray
    ): SenderRecord<ByteBuffer, ByteBuffer, ByteBuffer> {
        val msg = csvRecord.base64Encoded(MESSAGE)
        schemaId.copyInto(msg, 1, 0, schemaId.size)

        val key = csvRecord.base64EncodedBuf(KEY)

        return SenderRecord.create(
            ProducerRecord(
                topic,
                partition,
                csvRecord.get(TIMESTAMP).toLong(),
                key,
                ByteBuffer.wrap(msg)
            ),
            key
        )
    }
}

data class TopicLoadFile(val topic: String, val partition: Int, val path: Path)