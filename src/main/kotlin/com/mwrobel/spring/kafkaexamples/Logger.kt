package com.mwrobel.spring.kafkaexamples

import org.slf4j.Logger
import org.slf4j.LoggerFactory

inline fun <reified T> logger(kl: T): Logger = LoggerFactory.getLogger(T::class.java)