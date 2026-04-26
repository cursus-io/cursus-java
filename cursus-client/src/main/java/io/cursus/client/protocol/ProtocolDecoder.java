package io.cursus.client.protocol;

import io.cursus.client.exception.CursusProtocolException;
import io.cursus.client.message.AckResponse;
import io.cursus.client.message.CursusMessage;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes Cursus binary wire protocol messages. Matches Go SDK's protocol.go DecodeBatchMessages
 * and JSON AckResponse parsing.
 */
public final class ProtocolDecoder {

  private ProtocolDecoder() {}

  public static AckResponse decodeAckResponse(byte[] data) {
    String json = new String(data, StandardCharsets.UTF_8).trim();
    return AckResponse.builder()
        .status(extractJsonString(json, "status"))
        .lastOffset(extractJsonLong(json, "last_offset"))
        .producerEpoch(extractJsonInt(json, "producer_epoch"))
        .producerId(extractJsonString(json, "producer_id"))
        .seqStart(extractJsonLong(json, "seq_start"))
        .seqEnd(extractJsonLong(json, "seq_end"))
        .leader(extractJsonString(json, "leader"))
        .errorMsg(extractJsonString(json, "error"))
        .build();
  }

  public static List<CursusMessage> decodeBatchMessages(byte[] data) {
    ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);

    int magic = Short.toUnsignedInt(buf.getShort());
    if (magic != ProtocolEncoder.BATCH_MAGIC) {
      throw new CursusProtocolException("Invalid batch magic: 0x" + Integer.toHexString(magic));
    }

    skipLengthPrefixed(buf); // topic
    buf.getInt(); // partition (int32)
    skipByteLengthPrefixed(buf); // acks (uint8 length prefix)
    buf.get(); // idempotent flag
    buf.getLong(); // seqStart
    buf.getLong(); // seqEnd

    int messageCount = buf.getInt();
    List<CursusMessage> messages = new ArrayList<>(messageCount);

    for (int i = 0; i < messageCount; i++) {
      messages.add(decodeMessageFromBatch(buf));
    }

    return messages;
  }

  public static boolean isErrorResponse(String response) {
    return response != null
        && (response.startsWith("ERROR:") || response.startsWith("ERROR "));
  }

  public static boolean isNotLeaderResponse(String response) {
    return response != null && response.contains("NOT_LEADER");
  }

  public static boolean isRebalanceRequired(String response) {
    return response != null
        && (response.contains("REBALANCE_REQUIRED") || response.contains("GEN_MISMATCH"));
  }

  private static CursusMessage decodeMessageFromBatch(ByteBuffer buf) {
    long offset = buf.getLong();
    long seqNum = buf.getLong();
    String producerId = readLengthPrefixed(buf);
    String key = readLengthPrefixed(buf);
    long epoch = buf.getLong();
    int payloadLen = buf.getInt();
    byte[] payloadBytes = new byte[payloadLen];
    buf.get(payloadBytes);
    String payload = new String(payloadBytes, StandardCharsets.UTF_8);
    String eventType = readLengthPrefixed(buf);
    long schemaVersion = Integer.toUnsignedLong(buf.getInt());
    long aggregateVersion = buf.getLong();
    String metadata = readLengthPrefixed(buf);

    return CursusMessage.builder()
        .offset(offset)
        .seqNum(seqNum)
        .producerId(producerId)
        .key(key)
        .epoch((int) epoch)
        .payload(payload)
        .eventType(eventType)
        .schemaVersion(schemaVersion)
        .aggregateVersion(aggregateVersion)
        .metadata(metadata)
        .build();
  }

  private static String readLengthPrefixed(ByteBuffer buf) {
    int len = Short.toUnsignedInt(buf.getShort());
    byte[] bytes = new byte[len];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static void skipLengthPrefixed(ByteBuffer buf) {
    int len = Short.toUnsignedInt(buf.getShort());
    buf.position(buf.position() + len);
  }

  private static void skipByteLengthPrefixed(ByteBuffer buf) {
    int len = Byte.toUnsignedInt(buf.get());
    buf.position(buf.position() + len);
  }

  private static String extractJsonString(String json, String key) {
    String search = "\"" + key + "\":";
    int idx = json.indexOf(search);
    if (idx == -1) return null;
    int valueStart = idx + search.length();
    while (valueStart < json.length() && json.charAt(valueStart) == ' ') valueStart++;
    if (valueStart >= json.length()) return null;
    if (json.charAt(valueStart) == '"') {
      int end = json.indexOf('"', valueStart + 1);
      return end == -1 ? null : json.substring(valueStart + 1, end);
    }
    return null;
  }

  private static long extractJsonLong(String json, String key) {
    String search = "\"" + key + "\":";
    int idx = json.indexOf(search);
    if (idx == -1) return 0;
    int valueStart = idx + search.length();
    while (valueStart < json.length() && json.charAt(valueStart) == ' ') valueStart++;
    int valueEnd = valueStart;
    while (valueEnd < json.length()
        && (Character.isDigit(json.charAt(valueEnd)) || json.charAt(valueEnd) == '-')) valueEnd++;
    if (valueStart == valueEnd) return 0;
    return Long.parseLong(json.substring(valueStart, valueEnd));
  }

  private static int extractJsonInt(String json, String key) {
    return (int) extractJsonLong(json, key);
  }
}
