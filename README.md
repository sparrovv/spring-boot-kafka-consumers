# Spring boot kafka consumer example

This is an exploratory test app that checks how to achieve a few "standards" when writing a kafka consumer.

## What does it include?

- kafka batch consumer
- manual ACKs
- custom serialization
- ability to stop / start consumer through HTTP calls
- topic creation if doesn't exist
- integration test

### Kafka batch consumer

In certain scenarios batch consumer might be preferred, for instance when some external API supports batch operations.

## How to run

```
docker-compose up
./gradle bootRun
```

### Common scenarios

#### Consume one by one

Advantage:

- out of the box error handling.

Questions:

- when is it acked?
- does it run in sequence?
- how to define an error handler?
- how to ensure idempotent consumer?

### Batch

#### How to handle exceptions in batch

### DLQ

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
