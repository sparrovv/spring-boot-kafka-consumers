# Spring boot kafka consumer example

This test app explores [spring-kafka](https://docs.spring.io/spring-kafka/docs/2.5.6.RELEASE/reference/html/) capabilities and tries to define some easy to follow templates for creating kafka consuemrs in spring boot.

## What does it include?

- Standard methods of consuming messages in spring kafka.
  - One by one message listener
  - Batch listener
- JSON serializers
    - Handling serialization exceptions
- Ability to stop / start consumers through HTTP calls
- SeekToCurrent error handler with DLT(dead letter topic) recovery
- Topics creation if don't exist
- Integration tests with embedded kafka
- Manual ACKs ()

### One by one message listener

The simplest way to start with spring kafka is to implement one by one message listener as it has a few goodies that are easy to configure:

- out of the box error handling - https://docs.spring.io/spring-kafka/docs/current/reference/html/#seek-to-current  
- out of the box DLT recoverer - https://docs.spring.io/spring-kafka/docs/current/reference/html/#dead-letters

What do you need:

- configuration for kafka consumers
- configuration for spring kafka container factory
- configuration for kafka producer
- ...

### Kafka batch listener

In certain scenarios batch-consumer is preferred, for instance when some API supports batch operations and it makes consumer to perform better.

But in this case we need to know how to handle exceptions and what strategy to use.
We might need to implement our own DLT producer.

## How to run

```
docker-compose up
./gradle bootRun
```


## Todos / References

- deserialization error handling
- consumer error handling
  - https://www.confluent.io/blog/spring-for-apache-kafka-deep-dive-part-1-error-handling-message-conversion-transaction-support/
  - read more about https://docs.spring.io/spring-kafka/reference/html/#dead-letters
  - https://stackoverflow.com/questions/51831034/spring-kafka-how-to-retry-with-kafkalistener?rq=1
  - https://stackoverflow.com/questions/49507709/dead-letter-queue-dlq-for-kafka-with-spring-kafka
  - https://github.com/spring-projects/spring-kafka/blob/master/src/reference/asciidoc/testing.adoc
