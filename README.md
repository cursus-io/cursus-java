# Cursus Java Client

Java client library for the [Cursus](https://github.com/cursus-io/cursus) message broker with first-class Spring Boot support.

## Features

- **Producer** — Partition batching, gzip compression, idempotent delivery, configurable linger time
- **Consumer** — Polling and streaming modes, consumer groups with modulo-based partition assignment
- **Consumer Groups** — Automatic join/sync/leave lifecycle with heartbeating and rebalance handling
- **Spring Boot Starter** — Zero-boilerplate auto-configuration and `@CursusListener` annotation
- **Virtual Threads** — Automatically uses `Executors.newVirtualThreadPerTaskExecutor()` on Java 21+, falls back to a fixed thread pool on Java 17–20

## Requirements

- Java 17 or later (Java 21+ recommended for Virtual Thread support)
- A running Cursus broker (default port `9000`)

## Quick Start

### Add the dependency

**Standalone (core library)**

```groovy
// build.gradle
dependencies {
    implementation 'io.cursus:cursus-client:0.1.0-SNAPSHOT'
}
```

**With Spring Boot**

```groovy
// build.gradle
dependencies {
    implementation 'io.cursus:cursus-spring-boot-starter:0.1.0-SNAPSHOT'
    implementation 'org.springframework.boot:spring-boot-starter-web'
}
```

### Send a message

```java
import io.cursus.client.config.Acks;
import io.cursus.client.config.CursusProducerConfig;
import io.cursus.client.producer.CursusProducer;
import java.util.List;

CursusProducerConfig config = CursusProducerConfig.builder()
        .brokers(List.of("localhost:9000"))
        .topic("my-topic")
        .partitions(4)
        .acks(Acks.ONE)
        .batchSize(500)
        .lingerMs(100)
        .build();

try (CursusProducer producer = new CursusProducer(config)) {
    long seq = producer.send("Hello, Cursus!");
    producer.flush();
    System.out.println("Sent seq=" + seq + ", acked=" + producer.getUniqueAckCount());
}
```

### Consume messages

```java
import io.cursus.client.config.ConsumerMode;
import io.cursus.client.config.CursusConsumerConfig;
import io.cursus.client.consumer.CursusConsumer;
import java.util.List;

CursusConsumerConfig config = CursusConsumerConfig.builder()
        .brokers(List.of("localhost:9000"))
        .topic("my-topic")
        .groupId("my-group")
        .consumerMode(ConsumerMode.STREAMING)
        .build();

CursusConsumer consumer = new CursusConsumer(config);
Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
consumer.start(msg ->
        System.out.printf("offset=%d key=%s payload=%s%n",
                msg.getOffset(), msg.getKey(), msg.getPayload()));
```

### Spring Boot — application.yml + @CursusListener

```yaml
cursus:
  brokers:
    - localhost:9000
  producer:
    topic: my-topic
    partitions: 4
    acks: one
    batch-size: 500
    linger-ms: 100
  consumer:
    topic: my-topic
    group-id: my-group
    mode: streaming
    auto-commit-interval: 5s
```

```java
import io.cursus.client.message.CursusMessage;
import io.cursus.spring.annotation.CursusListener;
import org.springframework.stereotype.Service;

@Service
public class MyEventHandler {

    @CursusListener(topic = "my-topic", groupId = "my-group")
    public void handle(CursusMessage message) {
        System.out.println("Received: " + message.getPayload());
    }
}
```

## Documentation

| Document | Description |
|---|---|
| [Getting Started](docs/getting-started.md) | Installation, first message, next steps |
| [Architecture](docs/architecture.md) | Module structure, data flow, Go SDK mapping |
| [Producer Guide](docs/producer-guide.md) | Batching, compression, idempotency, monitoring |
| [Consumer Guide](docs/consumer-guide.md) | Modes, groups, offsets, shutdown |
| [Spring Boot Integration](docs/spring-boot-integration.md) | Auto-configuration, @CursusListener |
| [Configuration Reference](docs/configuration-reference.md) | All properties with types and defaults |
| [Protocol](docs/protocol.md) | Wire format, commands, frame structure |
| [Virtual Threads](docs/virtual-threads.md) | Java 21+ auto-detection and behavior |

## Examples

| Example | Description |
|---|---|
| [standalone/](cursus-examples/standalone/) | 5 standalone Java examples (no Spring) |
| [spring-boot/](cursus-examples/spring-boot/) | Spring Boot REST app with producer and listener |

See each directory's README for run instructions.

## License

Apache License 2.0. See [LICENSE](LICENSE).
