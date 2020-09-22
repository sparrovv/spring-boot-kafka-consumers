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

## Questions

## Todos

- deserialization error handling
- consumer error handling
  - read more about https://docs.spring.io/spring-kafka/reference/html/#dead-letters
  - https://stackoverflow.com/questions/51831034/spring-kafka-how-to-retry-with-kafkalistener?rq=1
