package com.mwrobel.spring.kafkaexamples.service

import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import java.util.concurrent.CountDownLatch

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