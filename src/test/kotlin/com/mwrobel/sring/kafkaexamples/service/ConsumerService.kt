package com.mwrobel.sring.kafkaexamples.service

import com.mwrobel.sring.kafkaexamples.dto.MyMessage
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.stereotype.Component
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit.jupiter.SpringExtension
import java.util.*
import java.util.concurrent.CountDownLatch

@Component
class MessageProcessorTestImpl() : MessageProcessor {
    private val container = mutableListOf<String>()
    lateinit var latch: CountDownLatch

    override fun process(events: List<MyMessage>) {
        events.forEach{
            container.add(it.id)
            latch.countDown()
        }
    }

    override fun size(): Int {
        return container.size
    }
}

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
    private lateinit var identityStitchingProcessor: MessageProcessorTestImpl

    @Test
    fun `it consumes published messages in batches`() {
        identityStitchingProcessor.latch = CountDownLatch(10)

        (1..10).map{
            """{"id" : "${it}", "outcome": "foo"}"""
        }.forEach{
            this.template.send("test-topic", it)
        }
        identityStitchingProcessor.latch.await(4, java.util.concurrent.TimeUnit.SECONDS)

        assertEquals(10, identityStitchingProcessor.size())
    }

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

}