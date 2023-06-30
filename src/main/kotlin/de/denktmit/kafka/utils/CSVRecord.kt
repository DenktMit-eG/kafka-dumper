package de.denktmit.kafka.utils

import org.apache.commons.csv.CSVRecord
import java.nio.ByteBuffer
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi


@OptIn(ExperimentalEncodingApi::class)
fun CSVRecord.b64Get(name: String): ByteBuffer = ByteBuffer.wrap(Base64.decode(this.get(name)))
