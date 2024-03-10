package de.denktmit.kafka.config

import org.apache.kafka.common.serialization.ByteBufferDeserializer
import org.apache.kafka.common.serialization.ByteBufferSerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Lazy
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.SenderOptions
import java.nio.ByteBuffer


@Configuration
class KafkaConfig {
    @Bean
    @Lazy
    fun receiverOptions(props: KafkaProperties): ReceiverOptions<ByteBuffer, ByteBuffer> =
        ReceiverOptions.create<ByteBuffer, ByteBuffer>(props.buildConsumerProperties(null))
            .withKeyDeserializer(ByteBufferDeserializer())
            .withValueDeserializer(ByteBufferDeserializer())

    @Bean
    @Lazy
    fun senderOptions(props: KafkaProperties): SenderOptions<ByteBuffer, ByteBuffer> =
        SenderOptions.create<ByteBuffer, ByteBuffer>(props.buildProducerProperties(null))
            .withKeySerializer(ByteBufferSerializer())
            .withValueSerializer(ByteBufferSerializer())
}
