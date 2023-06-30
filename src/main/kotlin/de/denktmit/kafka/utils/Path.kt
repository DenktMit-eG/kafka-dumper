package de.denktmit.kafka.utils

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVRecord
import reactor.core.publisher.Flux
import reactor.kafka.receiver.ReceiverRecord
import java.nio.ByteBuffer
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.bufferedReader
import kotlin.io.path.bufferedWriter
import kotlin.io.path.exists


fun Path.writeCSV(
    csvFormat: CSVFormat,
    influx: Flux<Array<Any>>
): Flux<Array<Any>> {
    return Flux.using({
        if (this.exists()) {
            val writer = this.bufferedWriter(options = arrayOf(StandardOpenOption.APPEND, StandardOpenOption.CREATE))
            csvFormat.builder().setSkipHeaderRecord(true).build().print(writer)
        } else {
            val writer = this.bufferedWriter(options = arrayOf(StandardOpenOption.CREATE_NEW))
            csvFormat.print(writer)
        }
    }, { csvPrinter ->
        influx.map { m ->
            csvPrinter.printRecord(*m)
            csvPrinter.flush()
            m
        }
    }, { csvPrinter -> csvPrinter.close() })
}

fun Path.csvRead(csvFormat: CSVFormat, skipHeader: Boolean = true): Flux<CSVRecord> = Flux.using({
    val bufferedReader = this.bufferedReader()
    val csvParser = csvFormat.builder().setSkipHeaderRecord(skipHeader).build().parse(bufferedReader)
    csvParser
}, { csvParser ->
    Flux.fromIterable(csvParser)
}, { csvParser ->
    csvParser.close()
})