package io.cursus.client.protocol;

import io.cursus.client.exception.CursusProtocolException;
import io.cursus.client.message.AckResponse;
import io.cursus.client.message.CursusMessage;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes Cursus binary wire protocol messages. Matches Go SDK's protocol.go DecodeBatchMessages
 * and JSON AckResponse parsing.
 */
public final class ProtocolDecoder {

  private ProtocolDecoder() {}

  /** Broker-retention range returned with OFFSET_OUT_OF_RANGE responses. */
  public record OffsetRange(long requested, long earliest, long latest) {}

  /** UTF-8 STREAM_CONTROL frame sent on streaming connections before binary batch decoding. */
  public record StreamControl(
      String type, String reason, Long offset, Long requested, Long earliest, Long latest) {}

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

  public static Map<String, String> decodeOkFields(String response) {
    String resp = response == null ? "" : response.trim();
    Map<String, String> fields = new HashMap<>();
    if (!resp.startsWith("OK")) return fields;

    String[] parts = resp.split("\\s+");
    for (int i = 1; i < parts.length; i++) {
      int sep = parts[i].indexOf('=');
      if (sep > 0) {
        fields.put(parts[i].substring(0, sep), parts[i].substring(sep + 1));
      }
    }
    return fields;
  }

  public static long decodeOffsetResponse(String response) {
    String resp = response == null ? "" : response.trim();
    if (isErrorResponse(resp)) {
      throw new CursusProtocolException("Broker error: " + resp);
    }

    Map<String, String> fields = decodeOkFields(resp);
    if (fields.isEmpty()) {
      throw new CursusProtocolException("Unexpected offset response: " + resp);
    }
    String value = fields.get("offset");
    if (value == null) {
      throw new CursusProtocolException("Missing offset in response: " + resp);
    }
    return parseLongField(value, "offset", resp);
  }

  public static long decodeVersionResponse(String response) {
    String resp = response == null ? "" : response.trim();
    if (isErrorResponse(resp)) {
      throw new CursusProtocolException("Broker error: " + resp);
    }

    Map<String, String> fields = decodeOkFields(resp);
    if (fields.isEmpty()) {
      throw new CursusProtocolException("Unexpected version response: " + resp);
    }
    String value = fields.get("version");
    if (value == null) {
      throw new CursusProtocolException("Missing version in response: " + resp);
    }
    return parseLongField(value, "version", resp);
  }

  public static String decodeSnapshotResponse(String response) {
    String resp = response == null ? "" : response.trim();
    if (isErrorResponse(resp)) {
      throw new CursusProtocolException("Broker error: " + resp);
    }
    if ("OK snapshot=null".equals(resp)) return null;
    if (resp.startsWith("OK snapshot=")) {
      return resp.substring("OK snapshot=".length());
    }
    throw new CursusProtocolException("Unexpected snapshot response: " + resp);
  }

  public static boolean isErrorResponse(String response) {
    return response != null && (response.startsWith("ERROR:") || response.startsWith("ERROR "));
  }

  public static boolean isNotLeaderResponse(String response) {
    return response != null && response.contains("NOT_LEADER");
  }

  public static boolean isRebalanceRequired(String response) {
    return response != null
        && (response.contains("REBALANCE_REQUIRED") || response.contains("GEN_MISMATCH"));
  }

  public static boolean isCoordinatorFailure(String response) {
    return response != null
        && (response.contains("GEN_MISMATCH")
            || response.contains("NOT_OWNER")
            || response.contains("member_not_found")
            || response.contains("group_not_found")
            || response.contains("NOT_COORDINATOR"));
  }

  public static boolean isTerminalProducerError(String response) {
    if (response == null) return false;
    String lower = response.toLowerCase();
    return lower.contains("stale_producer_epoch")
        || lower.contains("stale producer epoch")
        || lower.contains("idempotency_gap")
        || lower.contains("idempotency gap")
        || lower.contains("idempotency error")
        || lower.contains("first message")
        || lower.contains("seqnum=1")
        || lower.contains("seqnum 1")
        || lower.contains("seq_num=1");
  }

  public static boolean isTerminalProducerError(AckResponse ack) {
    return ack != null && isTerminalProducerError(ack.getErrorMsg());
  }

  public static boolean isStaleProducerEpoch(String response) {
    return isTerminalProducerError(response);
  }

  public static boolean isStaleProducerEpoch(io.cursus.client.message.AckResponse ack) {
    return isTerminalProducerError(ack);
  }

  public static boolean isOffsetRegression(String response) {
    return response != null && response.trim().startsWith("ERROR: offset_regression");
  }

  public static boolean isOffsetOutOfRange(String response) {
    return response != null && response.trim().startsWith("ERROR: OFFSET_OUT_OF_RANGE");
  }

  public static OffsetRange decodeOffsetOutOfRange(String response) {
    Map<String, String> fields = decodeFields(response);
    if (!fields.containsKey("requested")
        || !fields.containsKey("earliest")
        || !fields.containsKey("latest")) {
      throw new CursusProtocolException("Missing offset range fields in response: " + response);
    }
    return new OffsetRange(
        parseLongField(fields.get("requested"), "requested", response),
        parseLongField(fields.get("earliest"), "earliest", response),
        parseLongField(fields.get("latest"), "latest", response));
  }

  public static boolean isStreamControlFrame(byte[] data) {
    if (data == null || data.length == 0) return false;
    String response = new String(data, StandardCharsets.UTF_8).trim();
    return response.startsWith("STREAM_CONTROL");
  }

  public static StreamControl decodeStreamControl(byte[] data) {
    String response = new String(data, StandardCharsets.UTF_8).trim();
    if (!response.startsWith("STREAM_CONTROL")) {
      throw new CursusProtocolException("Unexpected stream control frame: " + response);
    }
    Map<String, String> fields = decodeFields(response);
    return new StreamControl(
        fields.getOrDefault("type", ""),
        fields.getOrDefault("reason", ""),
        parseOptionalLong(fields.get("offset"), "offset", response),
        parseOptionalLong(fields.get("requested"), "requested", response),
        parseOptionalLong(fields.get("earliest"), "earliest", response),
        parseOptionalLong(fields.get("latest"), "latest", response));
  }

  private static Map<String, String> decodeFields(String response) {
    String resp = response == null ? "" : response.trim();
    Map<String, String> fields = new HashMap<>();
    String[] parts = resp.split("\\s+");
    for (int i = 1; i < parts.length; i++) {
      int sep = parts[i].indexOf('=');
      if (sep > 0) {
        fields.put(parts[i].substring(0, sep), parts[i].substring(sep + 1));
      }
    }
    return fields;
  }

  private static Long parseOptionalLong(String value, String field, String response) {
    return value == null ? null : parseLongField(value, field, response);
  }

  private static long parseLongField(String value, String field, String response) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new CursusProtocolException("Invalid " + field + " in response: " + response, e);
    }
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
