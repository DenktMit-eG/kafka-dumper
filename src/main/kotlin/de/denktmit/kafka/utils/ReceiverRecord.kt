package de.denktmit.kafka.utils

import reactor.kafka.receiver.ReceiverRecord
import java.nio.ByteBuffer
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi


@OptIn(ExperimentalEncodingApi::class)
fun ReceiverRecord<ByteBuffer, ByteBuffer>.b64Value(): String = Base64.encode(this.value().array())

@OptIn(ExperimentalEncodingApi::class)
fun ReceiverRecord<ByteBuffer, ByteBuffer>.b64Key(): String = Base64.encode(this.key().array())