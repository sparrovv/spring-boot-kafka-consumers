package com.mwrobel.sring.kafkaexamples.service

import com.mwrobel.sring.kafkaexamples.logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.stereotype.Service
import javax.annotation.PreDestroy

@Service
class KafkaConsumersManager{
    private val log = logger(this)
	@Autowired
	lateinit var kafkaListenerEndpointRegistry: KafkaListenerEndpointRegistry

	@PreDestroy
	fun preDestroy() = stop()

	@Value("\${main.consumer.id}")
	lateinit var consumerId:String

	fun start(): String {
		val listenerContainer: MessageListenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId)
		listenerContainer.start()
		return "start"
	}

	fun status(): String {
		val listenerContainer: MessageListenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId)
		if (listenerContainer.isRunning)
			return "running"
		else
			return "not-running"
	}

	fun stop(): String {
		val listenerContainer: MessageListenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId)
		listenerContainer.stop()
		log.info("stopping kafka consumer")

		return "stopped"
	}
}