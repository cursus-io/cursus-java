# Spring Boot Example

A Spring Boot 3 application that demonstrates the `cursus-spring-boot-starter`. It exposes a REST API for sending messages and uses `@CursusListener` to consume them.

## Prerequisites

- Java 17 or later
- A Cursus broker running on `localhost:9000`

Start the broker with Docker if you do not already have one:

```bash
docker run -d --name cursus -p 9000:9000 cursusio/cursus:latest
```

## Running

```bash
./gradlew :cursus-examples:spring-boot:bootRun
```

The application starts on port `8080`. You should see log output similar to:

```
Started CursusExampleApp in 2.3 seconds (process running for 2.6)
Starting @CursusListener: topic=example-topic, group=spring-example-group, method=handleMessage
```

## Endpoints

### Send a message

```bash
curl -X POST http://localhost:8080/api/messages \
  -H "Content-Type: text/plain" \
  -d "Hello from curl"
```

Response:

```json
{"seqNum":1,"totalAcked":1}
```

### Send a keyed message

```bash
curl -X POST "http://localhost:8080/api/messages?key=customer-42" \
  -H "Content-Type: text/plain" \
  -d "Order placed"
```

Messages with the same `key` always route to the same partition. Useful for ordering guarantees.

### Flush pending messages

Normally messages are flushed automatically when the batch is full or `lingerMs` elapses. Call this endpoint to force an immediate flush:

```bash
curl -X POST http://localhost:8080/api/messages/flush
```

Response:

```
Flushed. Total acked: 5
```

## Configuration Reference

The application is configured in `src/main/resources/application.yml`:

```yaml
server:
  port: 8080

cursus:
  brokers:
    - localhost:9000
  producer:
    topic: example-topic
    partitions: 4
    acks: one
    batch-size: 500
    linger-ms: 100
    compression-type: none
  consumer:
    topic: example-topic
    group-id: spring-example-group
    mode: streaming
    auto-commit-interval: 5s
    max-poll-records: 100
```

To point the example at a different broker, change `cursus.brokers`. To use a different topic, change both `cursus.producer.topic` and `cursus.consumer.topic` (and update the `@CursusListener` annotation to match, or set them via environment variables).

## @CursusListener Example

`EventListener.java` shows the minimal `@CursusListener` setup:

```java
import io.cursus.client.message.CursusMessage;
import io.cursus.spring.annotation.CursusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class EventListener {

    private static final Logger log = LoggerFactory.getLogger(EventListener.class);

    @CursusListener(topic = "example-topic", groupId = "spring-example-group")
    public void handleMessage(CursusMessage message) {
        log.info("Received message: offset={}, payload={}",
                message.getOffset(), message.getPayload());
    }
}
```

The `@CursusListener` annotation reads the broker address from `cursus.brokers` in the Spring environment automatically. You do not need to construct a `CursusConsumer` manually.

When the application shuts down, `CursusListenerRegistrar` sends `LEAVE_GROUP` and closes the consumer cleanly.
