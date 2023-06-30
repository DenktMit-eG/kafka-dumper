package de.denktmit.kafka.utils

import org.apache.commons.csv.CSVRecord
import java.nio.ByteBuffer
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi


@OptIn(ExperimentalEncodingApi::class)
fun CSVRecord.base64EncodedBuf(name: String): ByteBuffer = ByteBuffer.wrap(this.base64Encoded(name))
@OptIn(ExperimentalEncodingApi::class)
fun CSVRecord.base64Encoded(name: String): ByteArray = Base64.decode(this.get(name))
