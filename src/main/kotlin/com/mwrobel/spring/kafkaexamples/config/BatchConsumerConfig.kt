package com.mwrobel.spring.kafkaexamples.config

import com.mwrobel.spring.kafkaexamples.dto.MyEvent
import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler
import org.springframework.kafka.listener.SeekToCurrentErrorHandler
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.util.backoff.FixedBackOff
import java.util.*

@Configuration
@EnableKafka
class BatchConsumerConfig {
    @Value("\${main.batch-input.topic}")
    private lateinit var batchMainTopic: String

    @Autowired // this has values from the application.properties
    private lateinit var kafkaProperties: KafkaProperties

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, MyMessage> {
        val factory: ConcurrentKafkaListenerContainerFactory<String, MyMessage> = ConcurrentKafkaListenerContainerFactory()
        factory.setConsumerFactory(consumerFactory())
        factory.setBatchListener(true)
        factory.setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<String, MyMessage> {
        val props = kafkaProperties.buildConsumerProperties()

        // Settings needed for Deserialization ErrorHandling
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ErrorHandlingDeserializer::class.java
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java

        props[ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS] = JsonDeserializer::class.java.getName()

        props[JsonDeserializer.VALUE_DEFAULT_TYPE] =  MyMessage::class.java.getName()
        props[JsonDeserializer.TRUSTED_PACKAGES] = MyMessage::class.java.packageName

        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun createMainTopicIfDoesntExist(): NewTopic? {
        return NewTopic(batchMainTopic, 1, 1.toShort())
    }

    @Bean
    fun createMainTopicDLTIfDoesntExist(): NewTopic? {
        return NewTopic(batchMainTopic + ".DLT", 1, 1.toShort())
    }
}