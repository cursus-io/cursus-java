# Configuration Reference

## Producer Configuration (`CursusProducerConfig`)

Build with `CursusProducerConfig.builder()`. All fields have defaults; only `topic` is required.

| Property | Type | Default | Description |
|---|---|---|---|
| `brokers` | `List<String>` | `["localhost:9000"]` | One or more broker addresses in `host:port` format. The first address is used as the initial connection target. |
| `topic` | `String` | — | **Required.** The topic to publish to. |
| `partitions` | `int` | `4` | Number of partitions. Must match the partition count on the broker for correct routing. |
| `acks` | `Acks` | `Acks.ONE` | Acknowledgment level. `NONE` = fire-and-forget, `ONE` = leader ack, `ALL` = all replicas. |
| `batchSize` | `int` | `500` | Maximum number of messages in one batch. A batch is flushed when the buffer reaches this size or when `lingerMs` elapses. |
| `bufferSize` | `int` | `10000` | Maximum number of messages that can be held in a partition buffer before back-pressure applies. |
| `lingerMs` | `long` | `100` | Milliseconds to wait before flushing a non-full batch. Set to `0` to disable the linger timer. |
| `maxInflightRequests` | `int` | `5` | Maximum number of concurrent in-flight batch sends per producer instance. |
| `idempotent` | `boolean` | `false` | Attach sequence numbers to batches so the broker can deduplicate retries. Set `maxInflightRequests=1` when enabled. |
| `writeTimeoutMs` | `long` | `5000` | Milliseconds to wait for a broker ACK response before timing out a single batch attempt. |
| `flushTimeoutMs` | `long` | `30000` | Milliseconds `flush()` waits for all in-flight batches to complete. |
| `leaderStalenessMs` | `long` | `30000` | Time after which a cached leader address is considered stale and the producer re-resolves the leader. |
| `compressionType` | `String` | `"none"` | Compression algorithm name. Built-in: `"gzip"`. Use `"none"` to disable. Custom algorithms can be registered with `CompressionRegistry`. |
| `tlsCertPath` | `String` | `null` | Path to the TLS client certificate file. Leave null for plain TCP. |
| `tlsKeyPath` | `String` | `null` | Path to the TLS client private key file. Leave null for plain TCP. |
| `maxRetries` | `int` | `3` | Maximum number of retry attempts per batch after a failure. |
| `maxBackoffMs` | `long` | `10000` | Maximum backoff interval in milliseconds between retry attempts. Backoff starts at 100 ms and doubles on each attempt up to this cap. |

---

## Consumer Configuration (`CursusConsumerConfig`)

Build with `CursusConsumerConfig.builder()`. `topic` is required; `groupId` is required for consumer group operation.

| Property | Type | Default | Description |
|---|---|---|---|
| `brokers` | `List<String>` | `["localhost:9000"]` | Broker addresses in `host:port` format. |
| `topic` | `String` | — | **Required.** Topic to consume from. |
| `groupId` | `String` | `null` | Consumer group identifier. Required for partition assignment via JOIN_GROUP / SYNC_GROUP. |
| `consumerMode` | `ConsumerMode` | `STREAMING` | `STREAMING` — broker pushes messages over a persistent connection. `POLLING` — consumer polls with `CONSUME` commands. |
| `autoCommitInterval` | `Duration` | `5s` | How often the consumer commits its current offset to the broker. |
| `sessionTimeoutMs` | `long` | `30000` | Maximum time in milliseconds the broker waits for a heartbeat before considering the consumer dead. |
| `heartbeatIntervalMs` | `long` | `3000` | Interval in milliseconds between heartbeat sends. Should be well below `sessionTimeoutMs`. |
| `maxPollRecords` | `int` | `100` | Maximum number of messages returned in a single `CONSUME` poll response (polling mode only). |
| `batchSize` | `int` | `100` | Internal batch size hint used when requesting messages. |
| `immediateCommit` | `boolean` | `false` | When `true`, commit after each individual message rather than on the interval schedule. |
| `commitBatchSize` | `int` | `100` | Number of messages to accumulate before a commit when `immediateCommit` is `false`. |
| `commitIntervalMs` | `long` | `5000` | Commit interval in milliseconds (mirrors `autoCommitInterval` as a raw millisecond value). |
| `tlsCertPath` | `String` | `null` | Path to the TLS client certificate file. |
| `tlsKeyPath` | `String` | `null` | Path to the TLS client private key file. |
| `maxRetries` | `int` | `3` | Maximum number of reconnect/retry attempts. |
| `maxBackoffMs` | `long` | `10000` | Maximum backoff interval in milliseconds between retry attempts. |

---

## Spring Boot Properties (`CursusProperties`)

Configure via `application.yml` under the `cursus.*` prefix. Spring Boot's relaxed binding maps kebab-case YAML keys to camelCase Java fields automatically.

### Top-level

| YAML key | Java field | Type | Default | Description |
|---|---|---|---|---|
| `cursus.brokers` | `brokers` | `List<String>` | `["localhost:9000"]` | Broker addresses shared by both producer and consumer. |

### Producer (`cursus.producer.*`)

A `CursusProducer` bean is only created when `cursus.producer.topic` is present.

| YAML key | Java field | Type | Default | Description |
|---|---|---|---|---|
| `cursus.producer.topic` | `topic` | `String` | — | Topic to publish to. |
| `cursus.producer.partitions` | `partitions` | `int` | `4` | Partition count. |
| `cursus.producer.acks` | `acks` | `String` | `"one"` | `"none"`, `"one"`, or `"all"`. |
| `cursus.producer.batch-size` | `batchSize` | `int` | `500` | Batch size threshold. |
| `cursus.producer.linger-ms` | `lingerMs` | `long` | `100` | Linger time in milliseconds. |
| `cursus.producer.compression-type` | `compressionType` | `String` | `"none"` | `"none"` or `"gzip"`. |
| `cursus.producer.idempotent` | `idempotent` | `boolean` | `false` | Enable idempotent delivery. |
| `cursus.producer.max-inflight-requests` | `maxInflightRequests` | `int` | `5` | Concurrent in-flight sends. |
| `cursus.producer.flush-timeout-ms` | `flushTimeoutMs` | `long` | `30000` | Flush timeout in milliseconds. |
| `cursus.producer.tls-cert-path` | `tlsCertPath` | `String` | `null` | TLS certificate path. |
| `cursus.producer.tls-key-path` | `tlsKeyPath` | `String` | `null` | TLS key path. |

### Consumer (`cursus.consumer.*`)

A `CursusConsumer` bean is only created when `cursus.consumer.topic` is present.

| YAML key | Java field | Type | Default | Description |
|---|---|---|---|---|
| `cursus.consumer.topic` | `topic` | `String` | — | Topic to subscribe to. |
| `cursus.consumer.group-id` | `groupId` | `String` | `null` | Consumer group identifier. |
| `cursus.consumer.mode` | `mode` | `String` | `"streaming"` | `"streaming"` or `"polling"`. |
| `cursus.consumer.auto-commit-interval` | `autoCommitInterval` | `Duration` | `5s` | Offset commit interval. |
| `cursus.consumer.max-poll-records` | `maxPollRecords` | `int` | `100` | Max records per poll. |
| `cursus.consumer.session-timeout-ms` | `sessionTimeoutMs` | `long` | `30000` | Session timeout in milliseconds. |
| `cursus.consumer.heartbeat-interval-ms` | `heartbeatIntervalMs` | `long` | `3000` | Heartbeat interval in milliseconds. |
| `cursus.consumer.tls-cert-path` | `tlsCertPath` | `String` | `null` | TLS certificate path. |
| `cursus.consumer.tls-key-path` | `tlsKeyPath` | `String` | `null` | TLS key path. |
