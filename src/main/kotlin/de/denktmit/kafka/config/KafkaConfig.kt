package de.denktmit.kafka.config

import org.apache.kafka.clients.admin.AdminClient
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
    fun receiverOptions(props: KafkaProperties): ReceiverOptions<ByteBuffer, ByteBuffer> =
        ReceiverOptions.create<ByteBuffer, ByteBuffer>(props.buildConsumerProperties())
            .withKeyDeserializer(ByteBufferDeserializer())
            .withValueDeserializer(ByteBufferDeserializer())

    @Bean
    fun senderOptions(props: KafkaProperties): SenderOptions<ByteBuffer, ByteBuffer> =
        SenderOptions.create<ByteBuffer, ByteBuffer>(props.buildProducerProperties())
            .withKeySerializer(ByteBufferSerializer())
            .withValueSerializer(ByteBufferSerializer())


    @Bean
    fun adminClient(props: KafkaProperties): AdminClient = AdminClient.create(props.buildAdminProperties())
}