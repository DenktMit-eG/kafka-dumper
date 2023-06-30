package de.denktmit.kafka.command

import de.denktmit.kafka.config.CsvConfig.Companion.CSV_FORMAT
import de.denktmit.kafka.config.CsvConfig.Companion.DELTA
import de.denktmit.kafka.config.CsvConfig.Companion.KEY
import de.denktmit.kafka.config.CsvConfig.Companion.MESSAGE
import de.denktmit.kafka.config.KafkaCliConfiguration
import de.denktmit.kafka.utils.b64Get
import de.denktmit.kafka.utils.csvRead
import de.denktmit.kafka.utils.logEveryNthObservable
import org.apache.commons.csv.CSVRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.support.KafkaHeaders.PARTITION
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import reactor.core.publisher.Flux
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CountDownLatch


@ShellComponent
class KafkaTopicRestoreCommand(val senderOptions: SenderOptions<ByteBuffer, ByteBuffer>, val config: KafkaCliConfiguration) {
    companion object {
        val LOGGER: Logger = LoggerFactory.getLogger(KafkaTopicRestoreCommand::class.java)
    }

    @ShellMethod(key = ["restore"], value = "Restore a topic")
    fun restoreTopics(topics: List<String>) {
        val countDownLatch = CountDownLatch(1)

        Flux.fromIterable(topics).flatMap { topic ->
            KafkaSender.create(senderOptions).send(Path.of("")
                .csvRead(CSV_FORMAT)
                .delayUntil {
                    it.get(DELTA).toLong().let { delta ->
                        if (delta > 0) Flux.just(it)
                            .delayElements(java.time.Duration.ofMillis(delta)) else Flux.just(it)
                    }
                }
                .map { toRecord(it, topic) })
                .doOnError { e -> LOGGER.error("Send failed", e) }
                .logEveryNthObservable(
                    { rn, _ -> LOGGER.info("Messages {} send to {}", rn, topic) },
                    { rn -> LOGGER.info("Messages {} in total send to {}", rn, topic) },
                    100_000
                )
        }.doAfterTerminate { countDownLatch.countDown() }.subscribe()

        countDownLatch.await()
    }

    private fun toRecord(csvRecord: CSVRecord, topic: String) = SenderRecord.create(
        // DELTA, TIMESTAMP, OFFSET, KEY, MESSAGE
        ProducerRecord(topic, csvRecord.get(PARTITION).toInt(), csvRecord.b64Get(KEY), csvRecord.b64Get(MESSAGE)),
        csvRecord.b64Get(KEY)
    )
}
