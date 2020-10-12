# Spring boot kafka consumer example

This test app explores [spring-kafka](https://docs.spring.io/spring-kafka/docs/2.5.6.RELEASE/reference/html/) capabilities and tries to define some easy to follow templates for creating kafka consuemrs in spring boot.

## What does it include?

- Standard methods of consuming messages in spring kafka.
  - Batch listener
  - Single message listener
- JSON serializers
    - Handling serialization exceptions
- Ability to stop / start consumers through HTTP calls
- SeekToCurrent error handler with DLT(dead letter topic) recoverer
- Topics creation if don't exist
- Integration tests with embedded kafka
- Manual ACKs

### Standard kafka one by one message listener

The simplest way is to start with one by one message listener as it has a few goodies that are easy to configure:

- out of the box error handling.
- out of the box DLT recoverer

### Kafka batch listener

In certain scenarios batch consumer might be preferred, for instance when some external API supports batch operations.

## How to run

```
docker-compose up
./gradle bootRun
```

### Common scenarios

#### Consume one by one

### Batch

#### How to handle exceptions in batch

### Dead letter topic

## Error Handlers
    
See Seek To Current Container Error Handlers, Recovering Batch Error Handler, Publishing Dead-letter Records and After-rollback Processor for more information.

## Questions

How can we recover from the batch failure?:
- try to reprocess a batch again, at least for few times

## Todos / References

- deserialization error handling
- consumer error handling
  - https://www.confluent.io/blog/spring-for-apache-kafka-deep-dive-part-1-error-handling-message-conversion-transaction-support/
  - read more about https://docs.spring.io/spring-kafka/reference/html/#dead-letters
  - https://stackoverflow.com/questions/51831034/spring-kafka-how-to-retry-with-kafkalistener?rq=1
  - https://stackoverflow.com/questions/49507709/dead-letter-queue-dlq-for-kafka-with-spring-kafka
  - https://github.com/spring-projects/spring-kafka/blob/master/src/reference/asciidoc/testing.adoc
