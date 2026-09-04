package io.cursus.client.protocol;

import io.cursus.client.exception.CursusProtocolException;
import io.cursus.client.message.CursusMessage;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Encodes application payloads carried by Cursus Wire v2 frames. */
public final class ProtocolEncoder {

  public static final int BATCH_MAGIC = WireProtocol.BATCH_MAGIC;
  public static final int MAX_MESSAGE_SIZE = WireProtocol.MAX_FRAME_PAYLOAD;

  private static final int BATCH_VERSION = 2;
  private static final int BATCH_FLAG_IDEMPOTENT = 1;
  private static final int RECORD_VERSION = 2;
  private static final long RECORD_TIMESTAMP = 1L;
  private static final long RECORD_PRODUCER = 1L << 1;
  private static final long RECORD_KEY = 1L << 2;
  private static final long RECORD_EVENT_TYPE = 1L << 3;
  private static final long RECORD_SCHEMA_VERSION = 1L << 4;
  private static final long RECORD_AGGREGATE_VERSION = 1L << 5;
  private static final long RECORD_METADATA = 1L << 6;
  private static final long RECORD_TRANSACTIONAL_ID = 1L << 7;
  private static final long RECORD_TRANSACTION_STATE = 1L << 8;
  private static final long RECORD_TRANSACTION_MARKER = 1L << 9;
  private static final long RECORD_CONTROL_BATCH_TYPE = 1L << 10;
  private static final long RECORD_CONTROL_BATCH_VERSION = 1L << 11;
  private static final long RECORD_CONTROL_COORDINATOR_EPOCH = 1L << 12;
  private static final long RECORD_CONTROL_KEY = 1L << 13;
  private static final long RECORD_CONTROL_VALUE = 1L << 14;

  private ProtocolEncoder() {}

  /** The command codec carries routing separately, so a single-message payload is unchanged. */
  public static byte[] encodeMessage(String topic, byte[] payload) {
    return payload.clone();
  }

  public static byte[] encodeBatchMessages(
      String topic,
      int partition,
      List<CursusMessage> messages,
      String acks,
      boolean idempotent,
      long batchSeqNum) {
    validateAcks(acks);
    BinaryWriter writer = new BinaryWriter();
    writer.uint32(BATCH_MAGIC);
    writer.uint16(BATCH_VERSION);
    writer.uint16(idempotent ? BATCH_FLAG_IDEMPOTENT : 0);
    writer.string(topic);
    writer.int32(partition);
    writer.string(acks);
    writer.uint64(batchSeqNum);
    writer.uint64(messages.isEmpty() ? batchSeqNum : batchSeqNum + messages.size() - 1);
    writer.uint32(messages.size());
    for (CursusMessage message : messages) {
      writer.bytes(encodeRecord(topic, partition, message));
    }
    return writer.toByteArray();
  }

  private static byte[] encodeRecord(String topic, int partition, CursusMessage message) {
    validateTransactionFields(message);
    long presence = recordPresence(message);
    BinaryWriter writer = new BinaryWriter();
    writer.uint16(RECORD_VERSION);
    writer.uint64(presence);
    writer.string(topic);
    writer.int32(partition);
    writer.uint64(message.getOffset());
    writer.string(valueOrEmpty(message.getPayload()));
    if ((presence & RECORD_TIMESTAMP) != 0) writer.int64(message.getTimestamp());
    if ((presence & RECORD_PRODUCER) != 0) {
      writer.string(valueOrEmpty(message.getProducerId()));
      writer.uint64(message.getSeqNum());
      writer.int64(message.getEpoch());
    }
    if ((presence & RECORD_KEY) != 0) writer.string(message.getKey());
    if ((presence & RECORD_EVENT_TYPE) != 0) writer.string(message.getEventType());
    if ((presence & RECORD_SCHEMA_VERSION) != 0) {
      long version = message.getSchemaVersion();
      if (version < 0 || version > 0xffffffffL) {
        throw new CursusProtocolException("Schema version is outside uint32 range: " + version);
      }
      writer.uint32((int) version);
    }
    if ((presence & RECORD_AGGREGATE_VERSION) != 0) writer.uint64(message.getAggregateVersion());
    if ((presence & RECORD_METADATA) != 0) writer.string(message.getMetadata());
    if ((presence & RECORD_TRANSACTIONAL_ID) != 0) writer.string(message.getTransactionalId());
    if ((presence & RECORD_TRANSACTION_STATE) != 0) writer.string(message.getTransactionState());
    if ((presence & RECORD_TRANSACTION_MARKER) != 0) {
      writer.string(message.getTransactionMarker());
    }
    if ((presence & RECORD_CONTROL_BATCH_TYPE) != 0) writer.string(message.getControlBatchType());
    if ((presence & RECORD_CONTROL_BATCH_VERSION) != 0) {
      int version = message.getControlBatchVersion();
      if (version < Short.MIN_VALUE || version > Short.MAX_VALUE) {
        throw new CursusProtocolException(
            "Control batch version is outside int16 range: " + version);
      }
      writer.int16(version);
    }
    if ((presence & RECORD_CONTROL_COORDINATOR_EPOCH) != 0) {
      writer.int64(message.getControlBatchCoordinatorEpoch());
    }
    if ((presence & RECORD_CONTROL_KEY) != 0) writer.bytes(message.getControlBatchKey());
    if ((presence & RECORD_CONTROL_VALUE) != 0) writer.bytes(message.getControlBatchValue());
    return writer.toByteArray();
  }

  private static long recordPresence(CursusMessage message) {
    long presence = 0;
    if (message.getTimestamp() != 0) presence |= RECORD_TIMESTAMP;
    if (!valueOrEmpty(message.getProducerId()).isEmpty()
        || message.getSeqNum() != 0
        || message.getEpoch() != 0) presence |= RECORD_PRODUCER;
    if (!valueOrEmpty(message.getKey()).isEmpty()) presence |= RECORD_KEY;
    if (!valueOrEmpty(message.getEventType()).isEmpty()) presence |= RECORD_EVENT_TYPE;
    if (message.getSchemaVersion() != 0) presence |= RECORD_SCHEMA_VERSION;
    if (message.getAggregateVersion() != 0) presence |= RECORD_AGGREGATE_VERSION;
    if (!valueOrEmpty(message.getMetadata()).isEmpty()) presence |= RECORD_METADATA;
    if (!valueOrEmpty(message.getTransactionalId()).isEmpty()) presence |= RECORD_TRANSACTIONAL_ID;
    if (!valueOrEmpty(message.getTransactionState()).isEmpty())
      presence |= RECORD_TRANSACTION_STATE;
    if (!valueOrEmpty(message.getTransactionMarker()).isEmpty()) {
      presence |= RECORD_TRANSACTION_MARKER;
    }
    if (!valueOrEmpty(message.getControlBatchType()).isEmpty()) {
      presence |= RECORD_CONTROL_BATCH_TYPE;
    }
    if (message.getControlBatchVersion() != 0) presence |= RECORD_CONTROL_BATCH_VERSION;
    if (message.getControlBatchCoordinatorEpoch() != 0) {
      presence |= RECORD_CONTROL_COORDINATOR_EPOCH;
    }
    if (message.getControlBatchKey() != null) presence |= RECORD_CONTROL_KEY;
    if (message.getControlBatchValue() != null) presence |= RECORD_CONTROL_VALUE;
    return presence;
  }

  private static void validateTransactionFields(CursusMessage message) {
    String state = valueOrEmpty(message.getTransactionState());
    if (!(state.isEmpty()
        || "open".equals(state)
        || "committed".equals(state)
        || "aborted".equals(state))) {
      throw new CursusProtocolException("Invalid transaction state: " + state);
    }
    String marker = valueOrEmpty(message.getTransactionMarker());
    if (!(marker.isEmpty() || "commit".equals(marker) || "abort".equals(marker))) {
      throw new CursusProtocolException("Invalid transaction marker: " + marker);
    }
    String controlType = valueOrEmpty(message.getControlBatchType());
    if (!(controlType.isEmpty() || "transaction".equals(controlType))) {
      throw new CursusProtocolException("Invalid control batch type: " + controlType);
    }
  }

  private static void validateAcks(String acks) {
    if (!(acks.isEmpty()
        || "0".equals(acks)
        || "1".equals(acks)
        || "-1".equals(acks)
        || "all".equals(acks))) {
      throw new CursusProtocolException("Invalid acknowledgements: " + acks);
    }
  }

  private static String valueOrEmpty(String value) {
    return value == null ? "" : value;
  }

  private static final class BinaryWriter {
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final DataOutputStream data = new DataOutputStream(output);

    void uint16(int value) {
      write(() -> data.writeShort(value));
    }

    void int16(int value) {
      uint16(value);
    }

    void uint32(int value) {
      write(() -> data.writeInt(value));
    }

    void int32(int value) {
      uint32(value);
    }

    void uint64(long value) {
      write(() -> data.writeLong(value));
    }

    void int64(long value) {
      uint64(value);
    }

    void string(String value) {
      bytes(value.getBytes(StandardCharsets.UTF_8));
    }

    void bytes(byte[] value) {
      uint32(value.length);
      write(() -> data.write(value));
    }

    byte[] toByteArray() {
      byte[] value = output.toByteArray();
      if (value.length > MAX_MESSAGE_SIZE) {
        throw new CursusProtocolException("Wire v2 batch exceeds maximum frame payload");
      }
      return value;
    }

    private void write(IORunnable operation) {
      try {
        operation.run();
      } catch (IOException exception) {
        throw new CursusProtocolException("Failed to encode Wire v2 batch", exception);
      }
    }
  }

  @FunctionalInterface
  private interface IORunnable {
    void run() throws IOException;
  }
}
