package com.mwrobel.sring.kafkaexamples

import com.mwrobel.sring.kafkaexamples.service.KafkaConsumersManager
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.GetMapping
import javax.annotation.PreDestroy
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
	private val log = logger(this)

	@Autowired
	lateinit var kafkaConsumersManager: KafkaConsumersManager

	@Autowired
	lateinit var template: KafkaTemplate<String, String>

	@PreDestroy
	fun preDestroy() = stop()

	@GetMapping("/start")
	fun start(): String {
        kafkaConsumersManager.start()
		return "start"
	}

	@GetMapping("/stop")
	fun stop(): String {
		kafkaConsumersManager.stop()

		return "stopped"
	}
}

fun main(args: Array<String>) {
	runApplication<KafkaExamplesApplication>(*args)
}
