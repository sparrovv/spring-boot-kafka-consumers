package com.mwrobel.spring.kafkaexamples

import com.mwrobel.spring.kafkaexamples.service.BatchConsumerManager
import com.mwrobel.spring.kafkaexamples.service.MessageProcessor
import com.mwrobel.spring.kafkaexamples.service.TestMessageProcessor
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit.jupiter.SpringExtension
import java.util.concurrent.CountDownLatch


@ExtendWith(SpringExtension::class)
@SpringBootTest()
@DirtiesContext
@ActiveProfiles("test")
@EmbeddedKafka(topics = arrayOf("\${main.batch-input.topic}", "test-topic.dlq"))
class BatchConsumerServiceIntegrationTest() {
    @Value("\${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
    private lateinit var brokerAddresses: String

    @Autowired
    private lateinit var embeddedKafka: EmbeddedKafkaBroker

    @Autowired
    private lateinit var template: KafkaTemplate<String, String>

    @Autowired
    private lateinit var identityStitchingProcessor: MessageProcessor

    @Autowired
    private lateinit var kafkaManager : BatchConsumerManager

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
    fun `when there's a serialization exception it logs it`() {
        // this is not ideal :/ Must be a better way
        val msgProcessorr = identityStitchingProcessor as TestMessageProcessor
        msgProcessorr.latch = CountDownLatch(2)

        this.template.send("test-topic", """{"id" : "1", "outcome": "foo"}""")
        this.template.send("test-topic", """{"oo" : "1"}""")
        this.template.send("test-topic", """{"id" : "1", "outcome": "foo"}""")

        msgProcessorr.latch.await(2, java.util.concurrent.TimeUnit.SECONDS)

        assertEquals(2, identityStitchingProcessor.size())
    }

    @Test
    fun `when there's an exception, it retries and sends the batch to dlq topic`() {
        val msgProcessorr = identityStitchingProcessor as TestMessageProcessor
        msgProcessorr.maxNumberOfExceptions = 2
        msgProcessorr.latch = CountDownLatch(2)

        this.template.send("test-topic", """{"id" : "1", "outcome": "foo"}""")
        this.template.send("test-topic", """{"id" : "2", "outcome": "exception"}""")
        this.template.send("test-topic", """{"id" : "3", "outcome": "bar"}""")

        msgProcessorr.latch.await(2, java.util.concurrent.TimeUnit.SECONDS)

        assertEquals(2, identityStitchingProcessor.size())

        val consumer = createTestConsumer(groupName = "test-group", topic = "test-topic.dlq")
        val records = KafkaTestUtils.getRecords<String, String>(consumer)

        assertEquals(records.count(), 1)
    }

//    @Test
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

    @TestConfiguration
    class MessageProcessorConf {
        @Bean @Primary
        fun testProcessor(): MessageProcessor {
            return TestMessageProcessor()
        }
    }

    private fun createTestConsumer(groupName:String = "test-group", topic:String): Consumer<String, String> {
        val consumerProps: MutableMap<String, Any> = KafkaTestUtils
                .consumerProps(groupName, "true", embeddedKafka);
        consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        val cf: ConsumerFactory<String, String> = DefaultKafkaConsumerFactory(consumerProps)
        val consumer: Consumer<String, String> = cf.createConsumer()
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic)

        return consumer
    }
}