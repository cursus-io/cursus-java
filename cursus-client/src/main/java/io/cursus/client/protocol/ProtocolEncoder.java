package io.cursus.client.protocol;

import io.cursus.client.message.CursusMessage;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Encodes messages into the Cursus binary wire protocol. Matches Go SDK's protocol.go
 * EncodeMessage/EncodeBatchMessages.
 *
 * <p>Single message format: uint16(topicLen) + topic + payload Batch format: magic(0xBA7C) + header
 * + message array
 */
public final class ProtocolEncoder {

  public static final int BATCH_MAGIC = 0xBA7C;
  public static final int MAX_MESSAGE_SIZE = 64 * 1024 * 1024;

  private ProtocolEncoder() {}

  public static byte[] encodeMessage(String topic, byte[] payload) {
    byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
    ByteBuffer buf =
        ByteBuffer.allocate(2 + topicBytes.length + payload.length).order(ByteOrder.BIG_ENDIAN);
    buf.putShort((short) topicBytes.length);
    buf.put(topicBytes);
    buf.put(payload);
    return buf.array();
  }

  public static byte[] encodeBatchMessages(
      String topic,
      int partition,
      List<CursusMessage> messages,
      String acks,
      boolean idempotent,
      long batchSeqNum) {
    byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
    byte[] acksBytes = acks.getBytes(StandardCharsets.UTF_8);

    // Header: magic(2) + topic(2+len) + partition(4) + acks(1+len) + idempotent(1) + seqStart(8) +
    // seqEnd(8) + msgCount(4)
    int headerSize = 2 + 2 + topicBytes.length + 4 + 1 + acksBytes.length + 1 + 8 + 8 + 4;

    int messagesSize = 0;
    for (CursusMessage msg : messages) {
      messagesSize += estimateMessageSize(msg);
    }

    ByteBuffer buf = ByteBuffer.allocate(headerSize + messagesSize).order(ByteOrder.BIG_ENDIAN);

    buf.putShort((short) BATCH_MAGIC);
    putLengthPrefixedString(buf, topicBytes);
    buf.putInt(partition);
    putByteLengthPrefixedString(buf, acksBytes);
    buf.put((byte) (idempotent ? 1 : 0));
    buf.putLong(batchSeqNum);
    buf.putLong(batchSeqNum + messages.size() - 1);
    buf.putInt(messages.size());

    for (CursusMessage msg : messages) {
      encodeMessageInBatch(buf, msg);
    }

    byte[] result = new byte[buf.position()];
    buf.flip();
    buf.get(result);
    return result;
  }

  private static void encodeMessageInBatch(ByteBuffer buf, CursusMessage msg) {
    byte[] producerIdBytes = safeBytes(msg.getProducerId());
    byte[] keyBytes = safeBytes(msg.getKey());
    byte[] payloadBytes = safeBytes(msg.getPayload());
    byte[] eventTypeBytes = safeBytes(msg.getEventType());
    byte[] metadataBytes = safeBytes(msg.getMetadata());

    buf.putLong(msg.getOffset());
    buf.putLong(msg.getSeqNum());
    putLengthPrefixedString(buf, producerIdBytes);
    putLengthPrefixedString(buf, keyBytes);
    buf.putLong(msg.getEpoch());
    buf.putInt(payloadBytes.length);
    buf.put(payloadBytes);
    putLengthPrefixedString(buf, eventTypeBytes);
    buf.putInt((int) msg.getSchemaVersion());
    buf.putLong(msg.getAggregateVersion());
    putLengthPrefixedString(buf, metadataBytes);
  }

  private static int estimateMessageSize(CursusMessage msg) {
    return 8
        + 8
        + 2
        + safeBytes(msg.getProducerId()).length
        + 2
        + safeBytes(msg.getKey()).length
        + 8
        + 4
        + safeBytes(msg.getPayload()).length
        + 2
        + safeBytes(msg.getEventType()).length
        + 4
        + 8
        + 2
        + safeBytes(msg.getMetadata()).length;
  }

  private static void putLengthPrefixedString(ByteBuffer buf, byte[] bytes) {
    buf.putShort((short) bytes.length);
    buf.put(bytes);
  }

  private static void putByteLengthPrefixedString(ByteBuffer buf, byte[] bytes) {
    buf.put((byte) bytes.length);
    buf.put(bytes);
  }

  private static byte[] safeBytes(String s) {
    return (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
  }
}
