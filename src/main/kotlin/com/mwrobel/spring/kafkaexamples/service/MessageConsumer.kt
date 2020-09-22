package com.mwrobel.spring.kafkaexamples.service

import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import com.mwrobel.spring.kafkaexamples.logger
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.KafkaListenerErrorHandler
import org.springframework.kafka.listener.ListenerExecutionFailedException
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service

interface MessageProcessor {
    fun process(events: List<MyMessage>)
    fun size():Int
}

@Component("MessageConsumerErrorHandler")
class MessageConsumerErrorHandler : KafkaListenerErrorHandler {
    override fun handleError(message: Message<*>?, exception: ListenerExecutionFailedException?): Any {
        //@todo add something, or seek back, or ignore
        println(message)
        return "x"
    }
}

@Service
class MessageConsumer(public val processor: MessageProcessor) {
    val log = logger(this)

    @Value("\${main.input.topic}")
    lateinit var topic :String

    @KafkaListener(
            id = "\${main.consumer.id}",
            topics = arrayOf("\${main.input.topic}"),
            autoStartup = "\${main.autostart}"
//            errorHandler = "MessageConsumerErrorHandler"
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
