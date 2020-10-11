package com.mwrobel.spring.kafkaexamples.service

import com.mwrobel.spring.kafkaexamples.dto.Message
import java.lang.RuntimeException
import java.util.concurrent.CountDownLatch

class TestMessageProcessor() : MessageProcessor {
    private val container = mutableListOf<String>()
    lateinit var latch: CountDownLatch
    var maxNumberOfExceptions: Int = 2
    var numberOfExceptions = 0

    override fun process(events: List<Message>): ProcessResult {
        val notProcessed: MutableList<Message> = mutableListOf()

        events.asSequence().forEach lit@{
            println("I'm in a loop: ${it}")
            if (it.outcome == "exception" && numberOfExceptions < maxNumberOfExceptions) {
                // process should receive the number of exceptions somehow
                numberOfExceptions += 1
                throw RuntimeException("well, this won't work")
            }
            if (numberOfExceptions == maxNumberOfExceptions) {
                numberOfExceptions = 0
                notProcessed.add(it)
                // local return
                return@lit
            }
            container.add(it.id)
            latch.countDown()
        }

        return ProcessResult(events - notProcessed, notProcessed = notProcessed)
    }

    override fun size(): Int {
        return container.size
    }
}