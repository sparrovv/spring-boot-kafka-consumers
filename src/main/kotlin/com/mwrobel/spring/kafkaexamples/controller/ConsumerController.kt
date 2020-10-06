package com.mwrobel.spring.kafkaexamples.controller

import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import com.mwrobel.spring.kafkaexamples.service.KafkaConsumersManager
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import kotlin.random.Random

data class Outcome(val outcome: String)

@RestController
@RequestMapping("/api")
class ConsumerController {

    @Autowired
    lateinit var kafkaConsumersManager: KafkaConsumersManager

    @Autowired
    lateinit var template: KafkaTemplate<String, MyMessage>

    @Value("\${main.input.topic}")
    lateinit var topic: String

    @GetMapping("/start")
    fun start(): Outcome {
        kafkaConsumersManager.start()

        return Outcome("start")
    }

    @GetMapping("/stop")
    fun stop(): Outcome {
        kafkaConsumersManager.stop()

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