# Standalone Examples

Five self-contained Java programs that demonstrate the Cursus Java Client without any Spring dependency. Each example can be run directly with Gradle.

## Prerequisites

- Java 17 or later
- A Cursus broker running on `localhost:9000`

Start the broker with Docker if you do not already have one:

```bash
docker run -d --name cursus -p 9000:9000 cursusio/cursus:latest
```

## Examples

| Class | Description |
|---|---|
| `SimpleProducer` | Send 10 messages one at a time and print the ACK count |
| `SimpleConsumer` | Consume messages from a topic using streaming mode |
| `BatchProducer` | Send 10,000 messages with batching (size 500) and gzip compression |
| `KeyedProducer` | Send messages with routing keys so related messages land on the same partition |
| `ConsumerGroupExample` | Join a consumer group in polling mode; use multiple terminals to see partition assignment in action |

## Running the Examples

Run each example with the Gradle `run` task from the repository root. Replace `<MainClass>` with the fully-qualified class name from the table below.

**SimpleProducer** — sends 10 messages to `example-topic` and reports the total acked count:

```bash
./gradlew :cursus-examples:standalone:run \
  -PmainClass=io.cursus.examples.standalone.SimpleProducer
```

**SimpleConsumer** — connects to `example-topic` in group `example-group` and prints each message. Press Enter to stop:

```bash
./gradlew :cursus-examples:standalone:run \
  -PmainClass=io.cursus.examples.standalone.SimpleConsumer
```

**BatchProducer** — sends 10,000 messages to `events` topic with batch size 500, linger 100 ms, and gzip compression. Prints elapsed time and partition stats:

```bash
./gradlew :cursus-examples:standalone:run \
  -PmainClass=io.cursus.examples.standalone.BatchProducer
```

**KeyedProducer** — sends four messages to the `orders` topic, two keys (`customer-123` and `customer-456`). Messages with the same key always go to the same partition:

```bash
./gradlew :cursus-examples:standalone:run \
  -PmainClass=io.cursus.examples.standalone.KeyedProducer
```

**ConsumerGroupExample** — joins group `event-workers` on topic `events` in polling mode. Start two terminals running this example to see the broker assign different partitions to each instance. Press Ctrl+C to stop:

```bash
./gradlew :cursus-examples:standalone:run \
  -PmainClass=io.cursus.examples.standalone.ConsumerGroupExample
```

## Typical Workflow

To see end-to-end message flow, run the producer and consumer together:

**Terminal 1** — start the consumer:

```bash
./gradlew :cursus-examples:standalone:run \
  -PmainClass=io.cursus.examples.standalone.SimpleConsumer
```

**Terminal 2** — run the producer:

```bash
./gradlew :cursus-examples:standalone:run \
  -PmainClass=io.cursus.examples.standalone.SimpleProducer
```

You should see the consumer print the 10 messages sent by the producer.
