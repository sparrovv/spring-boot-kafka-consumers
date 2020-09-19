package com.mwrobel.sring.kafkaexamples.service

import com.mwrobel.sring.kafkaexamples.dto.MyMessage
import com.mwrobel.sring.kafkaexamples.logger
import org.springframework.stereotype.Service

@Service
class LogMessageProcessor : MessageProcessor{
    var processedMsgs = 0
    private val log = logger(this)

    override fun process(events: List<MyMessage>) {
        events.forEach{
            log.info(it.id)

            processedMsgs += 1
        }
    }

    override fun size(): Int {
        return processedMsgs
    }
}