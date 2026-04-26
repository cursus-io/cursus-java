# Architecture

## Module Structure

```
cursus-java (root Gradle project)
│
├── cursus-client                    Core library — no Spring dependency
│   ├── compression/                 CursusCompressor, GzipCompressor, CompressionRegistry
│   ├── config/                      CursusProducerConfig, CursusConsumerConfig, Acks, ConsumerMode
│   ├── connection/                  ConnectionManager, CursusFrameDecoder, CursusFrameEncoder,
│   │                                CursusClientHandler
│   ├── consumer/                    CursusConsumer, PartitionConsumer
│   ├── exception/                   CursusException, CursusConnectionException,
│   │                                CursusProducerClosedException, CursusNotLeaderException
│   ├── message/                     CursusMessage, AckResponse
│   ├── producer/                    CursusProducer, PartitionBuffer, BatchState
│   ├── protocol/                    ProtocolEncoder, ProtocolDecoder, CommandBuilder
│   └── util/                        ExecutorFactory, RuntimeDetector, FnvHash, Backoff
│
├── cursus-spring-boot-starter       Spring Boot auto-configuration layer
│   ├── annotation/                  @CursusListener, CursusListenerRegistrar (BeanPostProcessor)
│   └── autoconfigure/               CursusAutoConfiguration (@ConditionalOn…), CursusProperties
│
└── cursus-examples
    ├── standalone/                  5 standalone runnable examples
    └── spring-boot/                 Spring Boot REST app (ProducerController, EventListener)
```

## Layer Diagram

```mermaid
flowchart TB
    A["Public API\nCursusProducer · CursusConsumer · @CursusListener"]
    B["Configuration Layer\nCursusProducerConfig · CursusConsumerConfig · CursusProperties (Spring)"]
    C["Protocol Layer\nProtocolEncoder · ProtocolDecoder · CommandBuilder"]
    D["Connection Layer\nConnectionManager · CursusClientHandler"]
    E["Netty Pipeline\nCursusFrameDecoder (4-byte length prefix)\nCursusFrameEncoder (4-byte length prefix)"]
    F["Transport: TCP / NioEventLoopGroup"]
    G["Cursus Broker (Go)   port 9000"]

    A --> B --> C --> D --> E --> F --> G
```

## Producer Data Flow

```mermaid
sequenceDiagram
    participant App as Application
    participant P as CursusProducer
    participant PB as PartitionBuffer
    participant PE as ProtocolEncoder
    participant CC as CursusCompressor
    participant CM as ConnectionManager
    participant B as Cursus Broker

    App->>P: send(payload, key)
    Note over P: Partition selection<br/>key != null → FnvHash.partition(key, N)<br/>key == null → roundRobin % N
    P->>PB: add(payload, key) → seqNum
    P-->>App: returns seqNum

    alt batchSize reached
        PB->>PB: drain()
    else lingerMs timer fires
        PB->>PB: forceFlush()
    else producer.flush() called
        PB->>PB: forceFlush() all buffers
    end

    PB->>PE: encodeBatchMessages(...)
    Note over PE: magic 0xBA7C + header<br/>(topic, partition, acks,<br/>idempotent, seqStart, seqEnd,<br/>count) + per-message fields

    alt compressionType != "none"
        PE->>CC: compress(bytes)
        CC-->>PE: compressed bytes
    end

    PE->>CM: send(bytes)
    CM->>CM: resolveLeader()
    CM->>B: NioSocketChannel.writeAndFlush()

    B-->>CM: ACK response bytes (async)
    CM->>P: ProtocolDecoder.decodeAckResponse(bytes)

    alt status == "OK"
        P->>P: uniqueAckCount += batch.size()
    else NOT_LEADER
        P->>P: updateLeader(null), retry with backoff
    else error
        P->>P: retry up to maxRetries
    end
```

## Consumer Data Flow

```mermaid
sequenceDiagram
    participant B as Cursus Broker
    participant FD as CursusFrameDecoder
    participant CH as CursusClientHandler
    participant PC as PartitionConsumer
    participant PD as ProtocolDecoder
    participant H as Message Handler
    participant CS as Commit Scheduler
    participant CM as ConnectionManager

    B->>FD: TCP frame (4-byte length prefix + payload, max 64 MB)
    FD->>CH: stripped payload bytes
    CH->>CH: CompletableFuture<byte[]> resolved
    CH->>PC: bytes

    alt STREAMING mode
        Note over PC,B: STREAM topic partition offset<br/>Broker pushes frames continuously
    else POLLING mode
        Note over PC,B: CONSUME topic partition offset<br/>Client polls each loop iteration
    end

    PC->>PD: decodeBatchMessages(bytes)
    loop for each CursusMessage
        PD->>H: handler.accept(msg)
        PC->>PC: currentOffset = msg.getOffset() + 1
    end

    CS->>CS: autoCommitInterval fires
    CS->>CM: CommandBuilder.commit(topic, groupId, partition, offset)
    CM->>B: COMMIT command
```

## Go SDK Mapping

The Java client mirrors the Go SDK's public surface. Key equivalences:

| Go SDK (config.go / types.go)        | Java SDK                                     |
|--------------------------------------|----------------------------------------------|
| `PublisherConfig`                    | `CursusProducerConfig`                       |
| `ConsumerConfig`                     | `CursusConsumerConfig`                       |
| `Message` struct                     | `CursusMessage`                              |
| `AckResponse` struct                 | `AckResponse`                                |
| `EncodeMessage` / `EncodeBatch`      | `ProtocolEncoder.encodeBatchMessages()`      |
| `DecodeBatchMessages`                | `ProtocolDecoder.decodeBatchMessages()`      |
| `hash/fnv` for partition routing     | `FnvHash.partition()`                        |
| `CREATE`, `CONSUME`, `STREAM`, etc.  | `CommandBuilder.create()`, `.consume()`, etc.|
| `BATCH_MAGIC = 0xBA7C`               | `ProtocolEncoder.BATCH_MAGIC`                |
| `Acks.ONE`, `ALL`, `NONE`            | `Acks.ONE`, `Acks.ALL`, `Acks.NONE`          |
| Polling / Streaming consumer mode    | `ConsumerMode.POLLING` / `STREAMING`         |
