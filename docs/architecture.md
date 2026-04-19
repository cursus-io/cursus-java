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

```
┌───────────────────────────────────────────────────────┐
│  Public API                                           │
│  CursusProducer  ·  CursusConsumer  ·  @CursusListener│
├───────────────────────────────────────────────────────┤
│  Configuration Layer                                  │
│  CursusProducerConfig  ·  CursusConsumerConfig        │
│  CursusProperties (Spring)                            │
├───────────────────────────────────────────────────────┤
│  Protocol Layer                                       │
│  ProtocolEncoder  ·  ProtocolDecoder  ·  CommandBuilder│
├───────────────────────────────────────────────────────┤
│  Connection Layer                                     │
│  ConnectionManager  ·  CursusClientHandler            │
├───────────────────────────────────────────────────────┤
│  Netty Pipeline                                       │
│  CursusFrameDecoder (4-byte length prefix)            │
│  CursusFrameEncoder (4-byte length prefix)            │
├───────────────────────────────────────────────────────┤
│  Transport: TCP / NioEventLoopGroup                   │
├───────────────────────────────────────────────────────┤
│  Cursus Broker (Go)   port 9000                       │
└───────────────────────────────────────────────────────┘
```

## Producer Data Flow

```
Application calls producer.send(payload, key)
          │
          ▼
Partition selection
  key != null  →  FnvHash.partition(key, numPartitions)   [FNV-1a mod]
  key == null  →  roundRobinCounter % numPartitions
          │
          ▼
PartitionBuffer.add(payload, key)          returns seqNum
          │
          ▼
Buffer reaches batchSize?  ──yes──►  PartitionBuffer.drain()
lingerMs timer fires?      ──yes──►  PartitionBuffer.forceFlush()
producer.flush() called?   ──yes──►  all buffers forceFlush()
          │
          ▼
ProtocolEncoder.encodeBatchMessages(...)
  magic 0xBA7C + header (topic, partition, acks, idempotent,
  seqStart, seqEnd, count) + per-message fields
          │
          ▼
compressionType != "none"?  ──yes──►  CursusCompressor.compress(bytes)
          │
          ▼
ConnectionManager.send(bytes)
  → resolveLeader()  → NioSocketChannel.writeAndFlush()
          │
          ▼ (async CompletableFuture)
CursusClientHandler receives response bytes
          │
          ▼
ProtocolDecoder.decodeAckResponse(bytes)  →  AckResponse
  status == "OK"  →  uniqueAckCount += batch.size()
  NOT_LEADER      →  updateLeader(null), retry with backoff
  error           →  retry up to maxRetries
```

## Consumer Data Flow

```
Broker  ──TCP──►  CursusFrameDecoder  ──►  CursusClientHandler
                  (strips 4-byte length prefix, max 64 MB)
                          │
                          ▼
                  CompletableFuture<byte[]> resolved
                          │
                   PartitionConsumer
                          │
           ┌──────────────┴──────────────┐
           │ STREAMING mode              │ POLLING mode
           │ STREAM topic part offset    │ CONSUME topic part offset
           │ broker pushes frames        │ client polls each loop iter
           └──────────────┬──────────────┘
                          │
                  ProtocolDecoder.decodeBatchMessages(bytes)
                          │
                          ▼
                  for each CursusMessage → handler.accept(msg)
                          │
                          ▼
                  currentOffset = msg.getOffset() + 1
                          │
                  commitScheduler (autoCommitInterval)
                          │
                  CommandBuilder.commit(topic, groupId, partition, offset)
                  ConnectionManager.sendCommand(cmd)
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
