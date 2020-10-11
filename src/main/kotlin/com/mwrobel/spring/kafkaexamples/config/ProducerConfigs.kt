package com.mwrobel.spring.kafkaexamples.config

import com.mwrobel.spring.kafkaexamples.dto.MyEvent
import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer
import java.util.HashMap


@Configuration
class ProducerConfigs {
    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Value("\${main.batch-input.topic}")
    private lateinit var batchMainTopic: String

    @Value("\${main.single-input.topic}")
    private lateinit var singleMainTopic: String

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
        val configs = HashMap<String, Any>()

        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JsonSerializer::class.java

        return DefaultKafkaProducerFactory(configs)
    }

    fun kafkaProducerFactory(): ProducerFactory<String, MyMessage> {
        val configs = HashMap<String, Any>()

        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JsonSerializer::class.java

        return DefaultKafkaProducerFactory(configs)
    }

    fun kafkaProducerFactoryString(): ProducerFactory<String, String> {
        val configs = HashMap<String, Any>()

        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

        return DefaultKafkaProducerFactory(configs)
    }
}