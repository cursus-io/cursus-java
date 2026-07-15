package io.cursus.client.protocol;

import io.cursus.client.exception.CursusAuthenticationException;
import io.cursus.client.exception.CursusAuthorizationException;
import io.cursus.client.exception.CursusBrokerException;
import io.cursus.client.exception.CursusProducerFencedException;
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

  public record PartitionOffsetRange(
      int partition, long earliest, long latest, long leo, long hwm) {}

  public record ProducerSession(String transactionalId, String producerId, long epoch) {}

  public record TransactionStatus(
      String transactionalId, String state, int messages, int offsets) {}

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

  public static boolean isOkResponse(String response) {
    String resp = response == null ? "" : response.trim();
    return "OK".equals(resp) || resp.startsWith("OK ");
  }

  public static String decodeErrorCode(String response) {
    String resp = response == null ? "" : response.trim();
    if (!resp.startsWith("ERROR:")) return "";
    String[] parts = resp.split("\\s+", 3);
    return parts.length > 1 ? parts[1] : "";
  }

  public static Map<String, String> decodeErrorFields(String response) {
    String resp = response == null ? "" : response.trim();
    Map<String, String> fields = new HashMap<>();
    if (!resp.startsWith("ERROR:")) return fields;
    String[] parts = resp.split("\\s+");
    for (int i = 2; i < parts.length; i++) {
      int sep = parts[i].indexOf('=');
      if (sep > 0) {
        fields.put(parts[i].substring(0, sep), parts[i].substring(sep + 1).replace("\"", ""));
      }
    }
    return fields;
  }

  public static Map<String, String> requireOk(String response, String operation) {
    String resp = response == null ? "" : response.trim();
    if (isErrorResponse(resp)) {
      throw errorFromResponse(resp);
    }
    if (!isOkResponse(resp)) {
      throw new CursusProtocolException("Unexpected " + operation + " response: " + resp);
    }
    return decodeOkFields(resp);
  }

  public static CursusBrokerException errorFromResponse(String response) {
    String code = decodeErrorCode(response);
    Map<String, String> fields = decodeErrorFields(response);
    String lower = response == null ? "" : response.toLowerCase();
    if ("AUTHENTICATION_REQUIRED".equals(code) || "authentication_required".equals(code)) {
      return new CursusAuthenticationException(code, fields, response);
    }
    if ("NOT_AUTHORIZED_FOR_TOPIC".equals(code) || "AUTHORIZATION_DENIED".equals(code)) {
      return new CursusAuthorizationException(code, fields, response);
    }
    if (lower.contains("producer_fenced") || lower.contains("stale_producer_epoch")) {
      return new CursusProducerFencedException(code, fields, response);
    }
    return new CursusBrokerException(code, fields, response);
  }

  public static String decodeNotCoordinator(String response) {
    if (!"NOT_COORDINATOR".equals(decodeErrorCode(response))) return null;
    Map<String, String> fields = decodeErrorFields(response);
    String host = fields.get("host");
    String port = fields.get("port");
    return host != null && port != null ? host + ":" + port : null;
  }

  public static List<PartitionOffsetRange> decodeListOffsetsResponse(String response) {
    Map<String, String> fields = requireOk(response, "list offsets");
    String value = fields.get("offsets");
    if (value == null) {
      throw new CursusProtocolException("Missing offsets in response: " + response);
    }
    List<PartitionOffsetRange> result = new ArrayList<>();
    for (String entry : value.split(",")) {
      if (entry.isBlank()) continue;
      result.add(parseListOffsetEntry(entry, response));
    }
    return result;
  }

  public static ProducerSession decodeProducerSession(String response) {
    Map<String, String> fields = requireOk(response, "producer session");
    String transactionalId = fields.get("transactional_id");
    String producerId = fields.getOrDefault("producerId", fields.get("producer_id"));
    String epoch = fields.get("epoch");
    if (transactionalId == null || producerId == null || epoch == null) {
      throw new CursusProtocolException("Malformed producer session response: " + response);
    }
    return new ProducerSession(
        transactionalId, producerId, parseLongField(epoch, "epoch", response));
  }

  public static TransactionStatus decodeTransactionStatus(String response) {
    Map<String, String> fields = requireOk(response, "transaction status");
    String transactionalId = fields.get("transactional_id");
    String state = fields.get("state");
    if (transactionalId == null || state == null) {
      throw new CursusProtocolException("Malformed transaction status response: " + response);
    }
    return new TransactionStatus(
        transactionalId,
        state,
        Integer.parseInt(fields.getOrDefault("messages", "0")),
        Integer.parseInt(fields.getOrDefault("offsets", "0")));
  }

  private static PartitionOffsetRange parseListOffsetEntry(String entry, String response) {
    String[] parts = entry.split(":");
    if (parts.length != 5 || !parts[0].startsWith("P")) {
      throw new CursusProtocolException("Invalid list offsets entry: " + entry);
    }
    int partition = Integer.parseInt(parts[0].substring(1));
    Map<String, Long> values = new HashMap<>();
    for (int i = 1; i < parts.length; i++) {
      String[] kv = parts[i].split("=", 2);
      if (kv.length != 2) {
        throw new CursusProtocolException("Invalid list offsets field: " + entry);
      }
      values.put(kv[0], parseLongField(kv[1], kv[0], response));
    }
    for (String key : List.of("earliest", "latest", "leo", "hwm")) {
      if (!values.containsKey(key)) {
        throw new CursusProtocolException("Missing list offsets field " + key + ": " + entry);
      }
    }
    return new PartitionOffsetRange(
        partition,
        values.get("earliest"),
        values.get("latest"),
        values.get("leo"),
        values.get("hwm"));
  }

  public static Map<String, String> decodeOkFields(String response) {
    String resp = response == null ? "" : response.trim();
    Map<String, String> fields = new HashMap<>();
    if (!isOkResponse(resp)) return fields;

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
    return lower.contains("producer_fenced")
        || lower.contains("stale_producer_epoch")
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
