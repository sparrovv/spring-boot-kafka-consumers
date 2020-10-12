package com.mwrobel.spring.kafkaexamples

import com.mwrobel.spring.kafkaexamples.dto.MyEvent
import com.mwrobel.spring.kafkaexamples.service.BatchConsumerManager
import com.mwrobel.spring.kafkaexamples.service.MessageProcessor
import com.mwrobel.spring.kafkaexamples.service.TestMessageProcessor
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.AfterEach
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
@EmbeddedKafka(topics = arrayOf("\${main.single-input.topic}", "\${main.single-input.topic}.DLT"))
class SingleMessageConsumerServiceIntegrationTest() {
    @Value("\${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
    private lateinit var brokerAddresses: String

    @Value("\${main.single-input.topic}")
    private lateinit var mainTopic:String

    @Autowired
    private lateinit var embeddedKafka: EmbeddedKafkaBroker

    @Autowired
    private lateinit var template: KafkaTemplate<String, MyEvent>

    @Autowired
    private lateinit var identityStitchingProcessor: MessageProcessor

    @Autowired
    private lateinit var kafkaManager : BatchConsumerManager

    @AfterEach
    fun afterEach():Unit {
        val msgProcessor = identityStitchingProcessor as TestMessageProcessor

        msgProcessor.reset()
    }

    @Test
    fun `when no exceptions it consumes published messages in batches`() {
        val msgProcessor = identityStitchingProcessor as TestMessageProcessor
        msgProcessor.latch = CountDownLatch(10)

        (1..10).map{
            MyEvent(id = it.toString(), outcome = "foo") //"""{"id" : "${it}", "outcome": "foo"}"""
        }.forEach{
            this.template.send(mainTopic, it)
        }
        msgProcessor.latch.await(4, java.util.concurrent.TimeUnit.SECONDS)

        assertEquals(10, identityStitchingProcessor.size())
    }

    @Test
    fun `when there's an exception, it retries and sends the batch to dlt topic`() {
        val msgProcessorr = identityStitchingProcessor as TestMessageProcessor
        msgProcessorr.latch = CountDownLatch(2)
        val consumer = createTestConsumer(groupName = "test-group-1", topic = "${mainTopic}.DLT")

        template.send(mainTopic, MyEvent(id = "1", outcome = "foo"))
        template.send(mainTopic, MyEvent(id = "2", outcome = "error"))
        template.send(mainTopic, MyEvent(id = "3", outcome = "bar"))

        msgProcessorr.latch.await(30, java.util.concurrent.TimeUnit.SECONDS)

        assertEquals(2, identityStitchingProcessor.size())

        val records = KafkaTestUtils.getRecords<String, String>(consumer)

        assertEquals(1, records.count())
    }

    @TestConfiguration
    class MessageProcessorConf {
        @Bean @Primary
        fun testProcessor(): MessageProcessor {
            return TestMessageProcessor()
        }
    }

    private fun createTestConsumer(groupName:String = "test-group-2", topic:String): Consumer<String, String> {
        val consumerProps: MutableMap<String, Any> = KafkaTestUtils
                .consumerProps(groupName, "true", embeddedKafka);
        consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        val cf: ConsumerFactory<String, String> = DefaultKafkaConsumerFactory(consumerProps)
        val consumer: Consumer<String, String> = cf.createConsumer()
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic)

        return consumer
    }
}