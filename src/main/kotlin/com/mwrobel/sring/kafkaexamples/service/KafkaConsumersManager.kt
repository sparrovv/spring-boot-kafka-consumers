package com.mwrobel.sring.kafkaexamples.service

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.stereotype.Service

@Service
class KafkaConsumersManager{
	@Autowired
	lateinit var kafkaListenerEndpointRegistry: KafkaListenerEndpointRegistry

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

		return "stopped"
	}
}