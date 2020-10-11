package com.mwrobel.spring.kafkaexamples.dto

interface Message {
    val id:String
    val outcome:String
}

data class MyMessage(override val id: String, override val outcome: String) : Message