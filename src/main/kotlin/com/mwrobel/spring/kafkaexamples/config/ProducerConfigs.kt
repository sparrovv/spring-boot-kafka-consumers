package com.mwrobel.spring.kafkaexamples.config

import com.mwrobel.spring.kafkaexamples.dto.MyEvent
import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer


@Configuration
class ProducerConfigs {
    @Autowired // this has values from the application.properties
    private lateinit var kafkaProperties: KafkaProperties

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, MyMessage> {
        return KafkaTemplate(kafkaProducerFactory())
    }

    @Bean
    fun kafkaStringTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(kafkaProducerFactoryString())
    }

    @Bean
    fun kafkaTemplateMyEvent(): KafkaTemplate<String, MyEvent> {
        return KafkaTemplate(kafkaProducerFactoryMyEvent())
    }

    @Bean
    fun kafkaOperationsMyEvent(): KafkaOperations<String, MyEvent> {
        return KafkaTemplate(kafkaProducerFactoryMyEvent())
    }

    @Bean
    fun kafkaProducerFactoryMyEvent(): ProducerFactory<String, MyEvent> {
        val props = kafkaProperties.buildProducerProperties()
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JsonSerializer::class.java

        return DefaultKafkaProducerFactory(props)
    }

    fun kafkaProducerFactory(): ProducerFactory<String, MyMessage> {
        val props = kafkaProperties.buildProducerProperties()
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JsonSerializer::class.java

        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JsonSerializer::class.java

        return DefaultKafkaProducerFactory(props)
    }

    fun kafkaProducerFactoryString(): ProducerFactory<String, String> {
        val props = kafkaProperties.buildProducerProperties()

        return DefaultKafkaProducerFactory(props)
    }
}