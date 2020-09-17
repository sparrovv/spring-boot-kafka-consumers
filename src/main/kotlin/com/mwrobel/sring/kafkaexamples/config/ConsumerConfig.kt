package com.mwrobel.sring.kafkaexamples.config

import com.mwrobel.sring.kafkaexamples.dto.MyMessage
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.support.serializer.JsonDeserializer
import java.util.*

@Configuration
@EnableKafka
class ConsumerConfig {
    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, MyMessage> {
        val factory: ConcurrentKafkaListenerContainerFactory<String, MyMessage> = ConcurrentKafkaListenerContainerFactory()
        factory.setConsumerFactory(consumerFactory())
        factory.setBatchListener(true)
        // there are more options, explore later
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // @todo learn what does it do
//        factory.setBatchErrorHandler(BatchLoggingErrorHandler())

        return factory
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<String, MyMessage> {
        val props: MutableMap<String, Any?> = HashMap()

        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG]  = "10000"
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG]  = "false"

        return DefaultKafkaConsumerFactory(
                props,
                StringDeserializer(),
                JsonDeserializer(MyMessage::class.java, false)
        )
    }
}