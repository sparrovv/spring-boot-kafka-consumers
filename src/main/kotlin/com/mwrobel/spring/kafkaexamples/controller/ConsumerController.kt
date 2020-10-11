package com.mwrobel.spring.kafkaexamples.controller

import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import com.mwrobel.spring.kafkaexamples.service.BatchConsumerManager
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import kotlin.random.Random

data class Outcome(val outcome: String)

@RestController
@RequestMapping("/api")
class ConsumerController {

    @Autowired
    lateinit var batchConsumerManager: BatchConsumerManager

    @Autowired
    lateinit var template: KafkaTemplate<String, MyMessage>

    @Value("\${main.batch-input.topic}")
    lateinit var topic: String

    @GetMapping("/start")
    fun start(): Outcome {
        batchConsumerManager.start()

        return Outcome("start")
    }

    @GetMapping("/stop")
    fun stop(): Outcome {
        batchConsumerManager.stop()

        return Outcome("stop")
    }

    @GetMapping("/produce")
    fun produce(): Outcome {
        val message = MyMessage(id = Random.nextInt().toString(), outcome = "yo")
        val future = template.send(topic, message)
        future.get()

        return Outcome("stop")
    }
}