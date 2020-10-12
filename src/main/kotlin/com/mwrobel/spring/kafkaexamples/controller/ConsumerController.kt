package com.mwrobel.spring.kafkaexamples.controller

import com.mwrobel.spring.kafkaexamples.dto.MyEvent
import com.mwrobel.spring.kafkaexamples.dto.MyMessage
import com.mwrobel.spring.kafkaexamples.service.KafkaConsumersManager
import com.mwrobel.spring.kafkaexamples.service.MessageProcessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import kotlin.random.Random

data class Outcome(val outcome: String)

@RestController
@RequestMapping("/api")
class ConsumerController {

    @Autowired
    lateinit var msgProcessor: MessageProcessor

    @Autowired
    @Qualifier("batchManager")
    lateinit var batchConsumerManager: KafkaConsumersManager

    @Autowired
    @Qualifier("oneByOneManager")
    lateinit var oneByOneMsgConsumer: KafkaConsumersManager

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
            @RequestParam(name = "id", required = true) consumer_id: String
    ): Outcome {
        if(consumer_id == batchConsumerManager.consumerId){
            batchConsumerManager.start()
        } else {
            oneByOneMsgConsumer.start()
        }

        return Outcome("start")
    }

    @GetMapping("/stop")
    fun stop(
            @RequestParam(name = "id", required = true) consumer_id: String
    ): Outcome {
        if(consumer_id == batchConsumerManager.consumerId){
            batchConsumerManager.stop()
        } else {
            oneByOneMsgConsumer.stop()
        }

        return Outcome("stop")
    }

    @GetMapping("/status")
    fun status(
            @RequestParam(name = "id", required = true) consumer_id: String
    ): Outcome {
        val status = if(consumer_id == batchConsumerManager.consumerId){
            batchConsumerManager.status()
        } else {
            oneByOneMsgConsumer.status()
        }

        return Outcome(status)
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

