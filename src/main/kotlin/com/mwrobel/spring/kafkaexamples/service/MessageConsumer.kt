package com.mwrobel.spring.kafkaexamples.service

import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import com.mwrobel.spring.kafkaexamples.logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.KafkaListenerErrorHandler
import org.springframework.kafka.listener.ListenerExecutionFailedException
import org.springframework.kafka.listener.SeekToCurrentErrorHandler
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import java.io.ByteArrayInputStream
import java.io.ObjectInput
import java.io.ObjectInputStream

interface MessageProcessor {
    fun process(events: List<MyMessage?>)
    fun size():Int
}

//@Component("MessageConsumerErrorHandler")
//class MessageConsumerErrorHandler : KafkaListenerErrorHandler {
//    override fun handleError(message: Message<*>?, exception: ListenerExecutionFailedException?): Any {
//        //@todo add something, or seek back, or ignore
//        println(message)
//        return "x"
//    }
//}

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
    fun receive(msgs: List<ConsumerRecord<String, MyMessage>>, ack: Acknowledgment) {
        log.info("Consuming ${msgs.count()} messages from a '${topic}' topic")

         msgs
                 .filter{cr -> cr.value() == null}
                 .forEach{cr ->
                     val header = cr.headers()
                             .findLast { it.key() == "springDeserializerExceptionValue" }
                     if (header is RecordHeader){
                         val ex: DeserializationException = fromByteArray(header.value())
                         log.warn("Problematic message.value: ${ex.data}")
                     }
                     log.warn("Message with offset: ${cr.offset()} had msg null ${cr}")
                 }

        val msgs = msgs.map{cr -> cr.value()}

        processor.process(msgs)

        ack.acknowledge()
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> fromByteArray(byteArray: ByteArray): T {
        val byteArrayInputStream = ByteArrayInputStream(byteArray)
        val objectInput: ObjectInput
        objectInput = ObjectInputStream(byteArrayInputStream)
        val result = objectInput.readObject() as T
        objectInput.close()
        byteArrayInputStream.close()
        return result
    }

}
