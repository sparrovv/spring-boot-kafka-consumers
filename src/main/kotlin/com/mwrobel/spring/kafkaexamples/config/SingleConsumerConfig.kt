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
class SingleConsumerConfig {
    @Value("\${main.single-input.topic}")
    private lateinit var singleMainTopic: String

    @Autowired
    lateinit var kafkaOperationsMyEvent: KafkaOperations<String, MyEvent>

    @Autowired // this has values from the application.properties
    private lateinit var kafkaProperties: KafkaProperties

    // you need to name the bean if you have more concurent kafka listeners
    @Bean("singleListener")
    fun kafkaListenerContainerFactorySingle(): ConcurrentKafkaListenerContainerFactory<String, MyEvent> {
        val factory: ConcurrentKafkaListenerContainerFactory<String, MyEvent> = ConcurrentKafkaListenerContainerFactory()
        factory.consumerFactory = consumerFactorySingle()

        val backOffPolicy = FixedBackOff()
        backOffPolicy.interval = 1000
        backOffPolicy.maxAttempts = 3


        factory.setErrorHandler(
                SeekToCurrentErrorHandler(
                        DeadLetterPublishingRecoverer(kafkaOperationsMyEvent),
                        backOffPolicy
                )
        )

        return factory
    }

    @Bean
    fun consumerFactorySingle(): ConsumerFactory<String, MyEvent> {
        val props = kafkaProperties.buildConsumerProperties()

        // Settings needed for Deserialization ErrorHandling
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ErrorHandlingDeserializer::class.java
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java

        props[ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS] = JsonDeserializer::class.java.getName()

        props[JsonDeserializer.VALUE_DEFAULT_TYPE] =  MyEvent::class.java.getName()
        props[JsonDeserializer.TRUSTED_PACKAGES] = MyEvent::class.java.packageName

        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun createMainTopicIfDoesntExist3(): NewTopic? {
        return NewTopic(singleMainTopic, 1, 1.toShort())
    }

    @Bean
    fun createMainTopic3DLTIfDoesntExist(): NewTopic? {
        return NewTopic(singleMainTopic + ".DLT", 1, 1.toShort())
    }
}