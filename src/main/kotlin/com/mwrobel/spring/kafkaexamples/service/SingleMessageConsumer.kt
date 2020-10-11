package com.mwrobel.spring.kafkaexamples.service

import com.mwrobel.spring.kafkaexamples.dto.MyEvent
import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import com.mwrobel.spring.kafkaexamples.logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.stereotype.Service
import java.io.ByteArrayInputStream
import java.io.ObjectInput
import java.io.ObjectInputStream
import java.lang.RuntimeException

@Service
class SingleMessageConsumer(public val processor: MessageProcessor) {
    val log = logger(this)

    @Value("\${main.single-input.topic}")
    lateinit var topic :String

    @Autowired
    lateinit var sender: KafkaTemplate<String, MyEvent>

    @KafkaListener(
            id = "\${main.single-consumer.id}",
            topics = arrayOf("\${main.single-input.topic}"),
            autoStartup = "\${main.autostart}",
            containerFactory = "singleListener"
    )
    fun receive(msg: ConsumerRecord<String, MyEvent>) {
        log.info("Message received: ${msg}")

        if (msg.value().outcome.contains("error")){
            throw RuntimeException("I won't recover for now")
        }

        processor.process(listOf(msg.value()))
    }
}
