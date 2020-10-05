package com.mwrobel.spring.kafkaexamples.service

import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import java.lang.RuntimeException
import java.util.concurrent.CountDownLatch

class TestMessageProcessor() : MessageProcessor {
    private val container = mutableListOf<String>()
    lateinit var latch: CountDownLatch
    var maxNumberOfExceptions: Int = 2
    var numberOfExceptions = 0

    override fun process(events: List<MyMessage?>) {
        events.filterNotNull().forEach{
            println("I'm in a loop: ${it}")
            if (it.outcome == "exception" && numberOfExceptions < maxNumberOfExceptions) {
                numberOfExceptions += 1
                throw RuntimeException("well, this won't work")
            }
            container.add(it.id)
            latch.countDown()
        }
    }

    override fun size(): Int {
        return container.size
    }
}