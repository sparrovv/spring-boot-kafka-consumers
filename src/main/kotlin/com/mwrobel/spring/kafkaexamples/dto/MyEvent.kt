package com.mwrobel.spring.kafkaexamples.dto

data class MyEvent(override val id: String, override val outcome: String) : Message