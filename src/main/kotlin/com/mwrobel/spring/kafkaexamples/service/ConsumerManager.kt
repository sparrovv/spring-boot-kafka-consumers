package com.mwrobel.spring.kafkaexamples.service

import com.mwrobel.spring.kafkaexamples.logger
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.KafkaMetric
import org.slf4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.MessageListenerContainer
import javax.annotation.PreDestroy

class KafkaConsumersManager(val consumerId: String){
    val log: Logger = logger(this)

    @Autowired
	lateinit var kafkaListenerEndpointRegistry: KafkaListenerEndpointRegistry

	@PreDestroy
	fun preDestroy() = stop()

	fun start(): String {
		val listenerContainer: MessageListenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId)
		listenerContainer.start()
		return "start"
	}

	fun status(): String {
		val listenerContainer: MessageListenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId)
		return if (listenerContainer.isRunning)
			"running"
		else
			"not-running"
	}

	fun metrics(): MutableMap<String, MutableMap<MetricName, KafkaMetric>> {
		val listenerContainer: MessageListenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId)
        return listenerContainer.metrics() as MutableMap<String, MutableMap<MetricName, KafkaMetric>>
	}

	fun stop(): String {
		val listenerContainer: MessageListenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId)
		listenerContainer.stop()
		log.info("stopping kafka consumer")

		return "stopped"
	}
}

@Configuration
class ConsumerManagers {
	@Value("\${main.batch-consumer.id}")
	lateinit var batchConsumerId: String

	@Value("\${main.single-consumer.id}")
	lateinit var oneByOneConsumerId: String

	@Bean("batchManager")
	fun batchConsumerManager(): KafkaConsumersManager{
		return KafkaConsumersManager(batchConsumerId)
	}

	@Bean("oneByOneManager")
	fun oneByOneConsumerManager(): KafkaConsumersManager{
		return KafkaConsumersManager(oneByOneConsumerId)
	}
}
