package com.mwrobel.spring.kafkaexamples.controller

import com.mwrobel.spring.kafkaexamples.dto.MyEvent
import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import com.mwrobel.spring.kafkaexamples.service.BatchConsumerManager
import com.mwrobel.spring.kafkaexamples.service.MessageProcessor
import com.mwrobel.spring.kafkaexamples.service.SingleMsgConsumerManager
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import kotlin.math.sin
import kotlin.random.Random

data class Outcome(val outcome: String)

@RestController
@RequestMapping("/api")
class ConsumerController {

    @Autowired
    lateinit var msgProcessor: MessageProcessor

    @Autowired
    lateinit var batchConsumerManager: BatchConsumerManager

    @Autowired
    lateinit var singleConsumerManager: SingleMsgConsumerManager

    @Autowired
    lateinit var template: KafkaTemplate<String, MyMessage>

    @Autowired
    lateinit var eventTemplate: KafkaTemplate<String, MyEvent>

    @Value("\${main.batch-input.topic}")
    lateinit var topic: String

    @Value("\${main.single-input.topic}")
    lateinit var mainSingleTopic: String

    @GetMapping("/start")
    fun start(
            @RequestParam(name = "id", required = true, defaultValue = "none") consumer_id: String
    ): Outcome {
        if(consumer_id == batchConsumerManager.consumerId){
            batchConsumerManager.start()
        } else {
            singleConsumerManager.start()
        }

        return Outcome("start")
    }

    @GetMapping("/stop")
    fun stop(
            @RequestParam(name = "id", required = true, defaultValue = "none") consumer_id: String
    ): Outcome {
        if(consumer_id == batchConsumerManager.consumerId){
            batchConsumerManager.stop()
        } else {
            singleConsumerManager.stop()
        }

        return Outcome("stop")
    }

    @GetMapping("/produceMessage")
    fun produceMessage(): Outcome {
        val message = MyMessage(id = Random.nextInt().toString(), outcome = "yo")
        val future = template.send(topic, message)
        future.get()

        return Outcome("produced")
    }

    @GetMapping("/produceEvent")
    fun produceEvent(): Outcome {
        val message = MyEvent(id = Random.nextInt().toString(), outcome = "hey")
        val future = eventTemplate.send(mainSingleTopic, message)
        future.get()

        return Outcome("produced")
    }

    @GetMapping("/stats")
    fun statsSingle(): Stats {
        return Stats(msgProcessor.size())
    }
}

data class Stats(val size: Int)

