package com.mwrobel.spring.kafkaexamples

import com.mwrobel.spring.kafkaexamples.service.KafkaConsumersManager
import com.mwrobel.spring.kafkaexamples.service.MessageProcessor
import com.mwrobel.spring.kafkaexamples.service.TestMessageProcessor
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit.jupiter.SpringExtension
import java.util.*
import java.util.concurrent.CountDownLatch


@ExtendWith(SpringExtension::class)
@SpringBootTest()
@DirtiesContext
@ActiveProfiles("test")
@EmbeddedKafka(topics = arrayOf("\${main.input.topic}"))

class ConsumerServiceIntegrationTest() {
    @Value("\${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
    private lateinit var brokerAddresses: String

    @Autowired
    private lateinit var template: KafkaTemplate<String, String>

    @Autowired
    private lateinit var identityStitchingProcessor: MessageProcessor

    @Autowired
    private lateinit var kafkaManager : KafkaConsumersManager

    @Test
    fun `when no exceptions it consumes published messages in batches`() {
        val msgProcessor = identityStitchingProcessor as TestMessageProcessor
        msgProcessor.latch = CountDownLatch(10)

        (1..10).map{
            """{"id" : "${it}", "outcome": "foo"}"""
        }.forEach{
            this.template.send("test-topic", it)
        }
        msgProcessor.latch.await(4, java.util.concurrent.TimeUnit.SECONDS)

        assertEquals(10, identityStitchingProcessor.size())
    }

    @Test
    fun `when there's a serialization exception it returns nulls`() {
        // this is not ideal :/ Must be a better way
        val msgProcessorr = identityStitchingProcessor as TestMessageProcessor
        msgProcessorr.latch = CountDownLatch(2)

        this.template.send("test-topic", """{"id" : "1", "outcome": "foo"}""")
        this.template.send("test-topic", """{"oo" : "1"}""")
        this.template.send("test-topic", """{"id" : "1", "outcome": "foo"}""")

        msgProcessorr.latch.await(2, java.util.concurrent.TimeUnit.SECONDS)

        assertEquals(2, identityStitchingProcessor.size())
        // how to do this
    }

    @Test
    fun `when there's an exception, it sends a message to dlq topic`() {
        val msgProcessorr = identityStitchingProcessor as TestMessageProcessor
        msgProcessorr.maxNumberOfExceptions = 2
        msgProcessorr.latch = CountDownLatch(2)

        this.template.send("test-topic", """{"id" : "1", "outcome": "foo"}""")
        this.template.send("test-topic", """{"id" : "2", "outcome": "exception"}""")
        this.template.send("test-topic", """{"id" : "3", "outcome": "bar"}""")

        msgProcessorr.latch.await(2, java.util.concurrent.TimeUnit.SECONDS)

        assertEquals(2, identityStitchingProcessor.size())
        // Need an assertion on dlq topic.
        //
    }

//    @Test @Disabled
//    fun `it retries later a message that has failed`() {
//        val msgProcessor = identityStitchingProcessor as MessageProcessorTestImpl
//        msgProcessor.latch = CountDownLatch(1)
//
//        this.template.send("test-topic", """{"id" : "${1}", "outcome": "fail-on-me-thre-times"}""")
//
//        msgProcessor.latch.await(1, java.util.concurrent.TimeUnit.SECONDS)
//
//        assertEquals(1, identityStitchingProcessor.size())
//    }

    @Bean
    fun kafkaProducerFactory(): ProducerFactory<String, String> {
        val configs = HashMap<String, Any>()
        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = this.brokerAddresses
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

        return DefaultKafkaProducerFactory(configs)
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(kafkaProducerFactory())
    }

    @TestConfiguration
    class MessageProcessorConf {
        @Bean @Primary
        fun testProcessor(): MessageProcessor {
            return TestMessageProcessor()
        }
    }
}