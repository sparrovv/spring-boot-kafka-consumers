package com.mwrobel.spring.kafkaexamples

import com.mwrobel.spring.kafkaexamples.service.KafkaConsumersManager
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
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
	@Qualifier("batchManager")
	lateinit var batchConsumerManager: KafkaConsumersManager

	@Autowired
	@Qualifier("oneByOneManager")
	lateinit var oneByOneMsgConsumer: KafkaConsumersManager


	@EventListener(ContextRefreshedEvent::class)
	fun contextRefreshedEvent():Unit {
        batchConsumerManager.start()
		oneByOneMsgConsumer.start()
	}
}

@SpringBootApplication
class KafkaExamplesApplication{
}

fun main(args: Array<String>) {
	runApplication<KafkaExamplesApplication>(*args)
}
