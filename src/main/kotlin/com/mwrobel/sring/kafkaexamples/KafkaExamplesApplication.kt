package com.mwrobel.sring.kafkaexamples

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaExamplesApplication

fun main(args: Array<String>) {
	runApplication<KafkaExamplesApplication>(*args)
}
