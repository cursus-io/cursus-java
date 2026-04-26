# Protocol

This document describes the wire protocol used between the Java client and the Cursus broker. The Java implementation mirrors the Go SDK's `protocol.go`.

## Transport

| Property | Value |
|---|---|
| Transport | TCP |
| Default port | `9000` |
| Frame delimiter | 4-byte big-endian length prefix |
| Maximum frame size | 64 MB (`64 * 1024 * 1024` bytes) |
| Encoding | UTF-8 for text commands; big-endian binary for batch messages |
| TLS | Optional; configured via `tlsCertPath` / `tlsKeyPath` |

## Frame Structure

Every message sent and received over the TCP connection is wrapped in a length-prefixed frame:

```
┌──────────────────────────────────────────────────────────────┐
│  4 bytes (uint32 big-endian)  │  N bytes (payload)           │
│  Frame length (N)             │  Command or batch body       │
└──────────────────────────────────────────────────────────────┘
```

```mermaid
flowchart LR
    subgraph Frame
        direction LR
        A["4 bytes\nuint32 big-endian\nFrame length (N)"]
        B["N bytes\nPayload\n(Command or batch body)"]
        A --> B
    end
    C["CursusFrameEncoder\n(outbound)"] --> Frame
    Frame --> D["CursusFrameDecoder\n(inbound)"]
```

The Netty pipeline handles framing transparently via `CursusFrameDecoder` (inbound) and `CursusFrameEncoder` (outbound). Application code never sees the length prefix.

## Commands

Text commands are UTF-8 strings. Fields are separated by a single space. The entire command string (without framing) is the payload.

| Command | Format | Description |
|---|---|---|
| `CREATE` | `CREATE <topic> <partitions>` | Create a new topic with the given partition count |
| `DELETE` | `DELETE <topic>` | Delete a topic and all its data |
| `LIST` | `LIST` | List all topics on the broker |
| `CONSUME` | `CONSUME <topic> <partition> <offset>` | Pull up to `maxPollRecords` messages starting at `offset` |
| `SUBSCRIBE` | `SUBSCRIBE <topic> <group>` | Subscribe a consumer group to a topic |
| `JOIN_GROUP` | `JOIN_GROUP <topic> <group> <consumerId>` | Join a consumer group; broker registers the member |
| `LEAVE_GROUP` | `LEAVE_GROUP <topic> <group> <consumerId>` | Leave a consumer group; triggers partition rebalance |
| `STREAM` | `STREAM <topic> <partition> <offset>` | Open a persistent push stream; broker sends batches as they arrive |
| `COMMIT` | `COMMIT <topic> <group> <partition> <offset>` | Commit a consumer offset for a partition |
| `HEARTBEAT` | `HEARTBEAT <topic> <group> <consumerId> <memberId> <generation>` | Keep-alive; broker may respond with `REBALANCE_REQUIRED` |
| `BATCH_COMMIT` | `BATCH_COMMIT <topic> <group> <memberId> <generation> <offsetsPayload>` | Commit offsets for multiple partitions in one round-trip |
| `SYNC_GROUP` | `SYNC_GROUP <topic> <group> <memberId> <generation>` | Fetch partition assignment after joining a group |

### Command Routing

```mermaid
flowchart TB
    A[Outbound frame payload] --> B{Starts with\n0xBA7C magic?}
    B -- yes --> C[Batch Message\nbinary encoding]
    B -- no --> D[Text Command\nUTF-8 string]

    C --> E[Producer path:\nProtocolEncoder.encodeBatchMessages]
    D --> F{Command type}

    F --> G[Topic management\nCREATE / DELETE / LIST]
    F --> H[Consumer group\nJOIN_GROUP / LEAVE_GROUP\nSYNC_GROUP / SUBSCRIBE]
    F --> I[Streaming & polling\nSTREAM / CONSUME]
    F --> J[Offset & heartbeat\nCOMMIT / BATCH_COMMIT\nHEARTBEAT]
```

## Batch Message Encoding

Producers send messages in binary batch format. The batch body (after the frame length prefix) has the following layout:

```
┌─────────────────────────────────────────────────────────────────────┐
│ HEADER                                                              │
│  uint16   magic = 0xBA7C                                            │
│  uint16   topicLen                                                  │
│  bytes    topic (UTF-8)                                             │
│  uint16   partitionLen                                              │
│  bytes    partition (UTF-8 decimal, e.g. "0")                      │
│  uint16   acksLen                                                   │
│  bytes    acks (UTF-8, e.g. "1", "0", "-1")                        │
│  uint8    idempotent (0x00 = false, 0x01 = true)                   │
│  int64    seqStart (big-endian)                                     │
│  int64    seqEnd   (big-endian)                                     │
│  int32    messageCount (big-endian)                                 │
├─────────────────────────────────────────────────────────────────────┤
│ MESSAGES  (repeated messageCount times)                             │
│  int64    offset                                                    │
│  int64    seqNum                                                    │
│  uint16   producerIdLen                                             │
│  bytes    producerId (UTF-8)                                        │
│  uint16   keyLen                                                    │
│  bytes    key (UTF-8)                                               │
│  int64    epoch                                                     │
│  int32    payloadLen                                                │
│  bytes    payload (UTF-8)                                           │
│  uint16   eventTypeLen                                              │
│  bytes    eventType (UTF-8)                                         │
│  int32    schemaVersion                                             │
│  int64    aggregateVersion                                          │
│  uint16   metadataLen                                               │
│  bytes    metadata (UTF-8)                                          │
└─────────────────────────────────────────────────────────────────────┘
```

All integer fields are big-endian. Length-prefixed strings use a `uint16` (2-byte) length followed by the UTF-8 bytes; an empty string is encoded as `0x0000` with no following bytes.

The magic value `0xBA7C` (decimal 47740) identifies the frame as a Cursus batch. Any frame that does not begin with this magic value is treated as a text command.

## ACK Response Format

After processing a batch, the broker returns a JSON response as a UTF-8 string (still framed with the 4-byte length prefix):

```json
{
  "status": "OK",
  "last_offset": 1023,
  "producer_epoch": 1,
  "producer_id": "abc-def-123",
  "seq_start": 100,
  "seq_end": 199,
  "leader": "localhost:9000",
  "error": ""
}
```

| Field | Type | Description |
|---|---|---|
| `status` | `string` | `"OK"` on success, `"PARTIAL"` if only some messages were stored, `"ERROR"` on failure |
| `last_offset` | `number` | Highest offset assigned to the batch |
| `producer_epoch` | `number` | Current producer epoch tracked by the broker |
| `producer_id` | `string` | Producer identifier echoed from the batch header |
| `seq_start` | `number` | First sequence number in the acknowledged batch |
| `seq_end` | `number` | Last sequence number in the acknowledged batch |
| `leader` | `string` | Current leader address; used by the client to update its cached leader |
| `error` | `string` | Error message when `status` is not `"OK"`; empty string otherwise |

Special text responses (not JSON) the client also handles:

| Response text | Meaning |
|---|---|
| Contains `NOT_LEADER` | The broker is not the current partition leader; client clears cached leader and retries |
| Contains `REBALANCE_REQUIRED` | Consumer group rebalance is needed; consumer stops and rejoins |
| Contains `GEN_MISMATCH` | Consumer's generation number is out of date; consumer rejoins |
| Starts with `ERROR:` | Hard broker error; the prefix is followed by an error description |

## Compression

When `compressionType` is not `"none"`, the entire encoded batch body is compressed before being wrapped in the length-prefixed frame:

```
Frame: [4-byte length] [compressed(batch body)]
```

The broker must understand the same compression algorithm. Built-in: `gzip`. Custom algorithms can be registered via `CompressionRegistry.getInstance().register(compressor)`.

The `CursusCompressor` interface:

```java
public interface CursusCompressor {
    String algorithmName();          // e.g. "gzip", "zstd"
    byte[] compress(byte[] data) throws Exception;
    byte[] decompress(byte[] data) throws Exception;
}
```
