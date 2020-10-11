package com.mwrobel.spring.kafkaexamples.service

import com.mwrobel.spring.kafkaexamples.logger
import org.slf4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.stereotype.Service
import javax.annotation.PreDestroy

interface KafkaConsumersManagerInt{
    val log: Logger// = logger(this)
	var consumerId:String
	var kafkaListenerEndpointRegistry: KafkaListenerEndpointRegistry

	@PreDestroy
	fun preDestroy() = stop()


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

@Service
class BatchConsumerManager : KafkaConsumersManagerInt {

	override val log = logger(this)
	@Value("\${main.batch-consumer.id}")
	override lateinit var consumerId: String
    @Autowired
	override lateinit var kafkaListenerEndpointRegistry: KafkaListenerEndpointRegistry
}

@Service
class SingleMsgConsumerManager : KafkaConsumersManagerInt {

	override val log = logger(this)
	@Value("\${main.single-consumer.id}")
	override lateinit var consumerId: String
	@Autowired
	override lateinit var kafkaListenerEndpointRegistry: KafkaListenerEndpointRegistry
}
