package com.mwrobel.sring.kafkaexamples.service

import com.mwrobel.sring.kafkaexamples.dto.MyMessage
import com.mwrobel.sring.kafkaexamples.logger
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service

interface MessageProcessor {
    fun process(events: List<MyMessage>)
    fun size():Int
}

@Service
class MessageConsumer(public val processor: MessageProcessor) {
    val log = logger(this)

    @Value("\${main.input.topic}")
    lateinit var topic :String

    @KafkaListener(
            topics = arrayOf("\${main.input.topic}"),
            groupId = "\${main.consumer.groupname}",
            autoStartup = "\${main.autostart}" // @todo would be good to stop / start on demand, same as in bar, not sure yet how?
    )
    fun receive(@Payload msgs: List<MyMessage>,
                          @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partitions: List<Int>,
                          @Header(KafkaHeaders.OFFSET) offsets: List<Long>,
                          @Header(KafkaHeaders.ACKNOWLEDGMENT) ack: Acknowledgment) {
        log.info("Consuming ${msgs.count()} messages from a '${topic}' topic")

        processor.process(msgs)

        ack.acknowledge()
    }
}
