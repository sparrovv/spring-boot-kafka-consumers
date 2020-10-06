package com.mwrobel.spring.kafkaexamples.service

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

class MyMaxException(message: String, val data: MyMessage): Exception(message)
data class ProcessResult(val processed: List<MyMessage>, val notProcessed: List<MyMessage>)
interface MessageProcessor {
    fun process(events: List<MyMessage>): ProcessResult
    fun size():Int
}


@Service
class MessageConsumer(public val processor: MessageProcessor) {
    val log = logger(this)

    @Value("\${main.input.topic}")
    lateinit var topic :String

    @Autowired
    lateinit var sender: KafkaTemplate<String, MyMessage>

    @KafkaListener(
            id = "\${main.consumer.id}",
            topics = arrayOf("\${main.input.topic}"),
            autoStartup = "\${main.autostart}"
//            errorHandler = "MessageConsumerErrorHandler"
    )
    fun receive(msgs: List<ConsumerRecord<String, MyMessage>>, ack: Acknowledgment) {
        log.info("Consuming ${msgs.count()} messages from a '${topic}' topic")

        logAnyNullMsgsAsTheyCanBeCausedBySerializationError(msgs)

        val notNullMsgs = msgs
                .map{cr -> cr.value()}
                .filterNotNull()

        val result = processor.process(notNullMsgs)

        result.notProcessed.forEach{
            sender.send(topic + ".dlq", it)
        }

        ack.acknowledge()
    }

    private fun logAnyNullMsgsAsTheyCanBeCausedBySerializationError(msgs: List<ConsumerRecord<String, MyMessage>>){
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
