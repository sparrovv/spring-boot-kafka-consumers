package com.mwrobel.spring.kafkaexamples.service

import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import com.mwrobel.spring.kafkaexamples.logger
import org.apache.tomcat.jni.Proc
import org.springframework.stereotype.Service

@Service
class LogMessageProcessor : MessageProcessor {
    var processedMsgs = 0
    private val log = logger(this)

    override fun process(events: List<MyMessage>): ProcessResult {
        events.forEach{
            if (it != null) {
                log.info(it.id)
                processedMsgs += 1
            } else {
                log.warn("There was a null entry")
            }
        }

        return ProcessResult(events, listOf())
    }

    override fun size(): Int {
        return processedMsgs
    }
}