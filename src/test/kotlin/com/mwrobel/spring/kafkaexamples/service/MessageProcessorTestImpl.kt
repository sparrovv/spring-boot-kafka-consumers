package com.mwrobel.spring.kafkaexamples.service

import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import org.springframework.kafka.listener.ListenerExecutionFailedException
import java.util.concurrent.CountDownLatch

class MessageProcessorTestImpl() : MessageProcessor {
    private val container = mutableListOf<String>()
    lateinit var latch: CountDownLatch

    override fun process(events: List<MyMessage>) {
        ListenerExecutionFailedException
        events.forEach{
            println("I'm in a loop: ${it}")
            container.add(it.id)
            latch.countDown()
        }
    }

    override fun size(): Int {
        return container.size
    }
}