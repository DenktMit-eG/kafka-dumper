package de.denktmit.kafka.config

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.QuoteMode

class CsvConfig {
    companion object {
        const val DELTA = "delta"
        const val OFFSET = "offset"
        const val TIMESTAMP = "TIMESTAMP"
        const val KEY = "key"
        const val MESSAGE = "message"
        private val HEADERS: Array<String> = arrayOf(DELTA, TIMESTAMP, OFFSET, KEY, MESSAGE)
        val CSV_FORMAT: CSVFormat =
            CSVFormat.DEFAULT.builder().setQuoteMode(QuoteMode.MINIMAL).setHeader(*HEADERS).build()
    }
}