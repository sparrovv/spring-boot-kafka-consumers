package com.mwrobel.sring.kafkaexamples

import com.mwrobel.sring.kafkaexamples.service.KafkaConsumersManager
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

/*
 This makes sure that Kafka Consumer starts on startup
 */
@Component
class StartupHousekeeper {
	@Autowired
	lateinit var kafkaConsumersManager: KafkaConsumersManager

	@EventListener(ContextRefreshedEvent::class)
	fun contextRefreshedEvent():Unit {
        kafkaConsumersManager.start()
	}
}

@SpringBootApplication
class KafkaExamplesApplication{
}

fun main(args: Array<String>) {
	runApplication<KafkaExamplesApplication>(*args)
}
