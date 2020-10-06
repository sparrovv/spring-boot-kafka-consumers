package com.mwrobel.spring.kafkaexamples.config

import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import java.util.*
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.retry.backoff.ExponentialBackOffPolicy
import org.springframework.retry.policy.AlwaysRetryPolicy
import org.springframework.retry.support.RetryTemplate

@Configuration
@EnableKafka
class ConsumerConfig {
    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Value("\${main.input.topic}")
    private lateinit var mainTopic: String

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, MyMessage> {
        val factory: ConcurrentKafkaListenerContainerFactory<String, MyMessage> = ConcurrentKafkaListenerContainerFactory()
        factory.setConsumerFactory(consumerFactory())
        factory.setBatchListener(true)
        factory.setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // needed to avoid committing offset
//        factory.setStatefulRetry(true);
        // so if there was an exception, I want to retry
//        val retryTemplate = RetryTemplate()
//        retryTemplate.setRetryPolicy(AlwaysRetryPolicy())
//        retryTemplate.setBackOffPolicy(ExponentialBackOffPolicy());
//
//        factory.setRetryTemplate(retryTemplate)
        return factory
    }

    @Bean
    fun kafkaProducerFactory(): ProducerFactory<String, MyMessage> {
        val configs = HashMap<String, Any>()

        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JsonSerializer::class.java

        return DefaultKafkaProducerFactory(configs)
    }

    @Bean
    fun kafkaProducerFactoryString(): ProducerFactory<String, String> {
        val configs = HashMap<String, Any>()

        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

        return DefaultKafkaProducerFactory(configs)
    }
    @Bean
    fun consumerFactory(): ConsumerFactory<String, MyMessage> {
        val props: MutableMap<String, Any?> = HashMap()

        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG]  = "10000"
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG]  = "false"

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
        return NewTopic(mainTopic, 1, 1.toShort())
    }

    @Bean
    fun createMainTopicDLTIfDoesntExist(): NewTopic? {
        return NewTopic(mainTopic + ".dlt", 1, 1.toShort())
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, MyMessage> {
        return KafkaTemplate(kafkaProducerFactory())
    }

    @Bean
    fun kafkaStringTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(kafkaProducerFactoryString())
    }
}