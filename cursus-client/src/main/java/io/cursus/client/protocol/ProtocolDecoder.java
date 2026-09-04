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

  private static final int RECORD_VERSION = 2;
  private static final long RECORD_TIMESTAMP = 1L;
  private static final long RECORD_PRODUCER = 1L << 1;
  private static final long RECORD_KEY = 1L << 2;
  private static final long RECORD_EVENT_TYPE = 1L << 3;
  private static final long RECORD_SCHEMA_VERSION = 1L << 4;
  private static final long RECORD_AGGREGATE_VERSION = 1L << 5;
  private static final long RECORD_METADATA = 1L << 6;
  private static final long RECORD_KNOWN_MASK = (1L << 15) - 1;

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
    String response = new String(data, StandardCharsets.UTF_8).trim();
    if (response.startsWith("ERROR:")) {
      String[] parts = response.split("\\s+");
      Map<String, String> fields = decodeFields(response);
      return AckResponse.builder()
          .status("ERROR")
          .errorMsg(response)
          .errorCode(parts.length > 1 ? parts[1] : "unknown")
          .errorClass(fields.remove("class"))
          .retryable(Boolean.parseBoolean(fields.remove("retryable")))
          .errorFields(Map.copyOf(fields))
          .build();
    }
    String json = response;
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
    BinaryReader reader = new BinaryReader(data);
    int magic = reader.uint32();
    if (magic != ProtocolEncoder.BATCH_MAGIC) {
      throw new CursusProtocolException("Invalid batch magic: 0x" + Integer.toHexString(magic));
    }
    int version = reader.uint16();
    if (version != WireProtocol.BATCH_VERSION) {
      throw new CursusProtocolException("Unsupported Wire v2 batch version: " + version);
    }
    int flags = reader.uint16();
    if ((flags & ~1) != 0) {
      throw new CursusProtocolException("Unknown Wire v2 batch flags: " + flags);
    }
    String topic = reader.string();
    int partition = reader.int32();
    validateAcks(reader.string());
    reader.uint64(); // seqStart
    reader.uint64(); // seqEnd

    int messageCount = reader.uint32();
    if (messageCount < 0) {
      throw new CursusProtocolException("Wire v2 batch message count exceeds int range");
    }
    List<CursusMessage> messages = new ArrayList<>(messageCount);
    for (int i = 0; i < messageCount; i++) {
      messages.add(decodeRecord(reader.bytes(), topic, partition));
    }
    reader.finish();
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
    return response != null && response.toLowerCase().contains("not_leader");
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
    if (ack == null) return false;
    String code = ack.getErrorCode();
    return "stale_producer_epoch".equalsIgnoreCase(code)
        || "idempotency_gap".equalsIgnoreCase(code)
        || isTerminalProducerError(ack.getErrorMsg());
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

  private static CursusMessage decodeRecord(byte[] data, String topic, int partition) {
    BinaryReader reader = new BinaryReader(data);
    int version = reader.uint16();
    if (version != RECORD_VERSION) {
      throw new CursusProtocolException("Unsupported Wire v2 record version: " + version);
    }
    long presence = reader.uint64();
    if ((presence & ~RECORD_KNOWN_MASK) != 0) {
      throw new CursusProtocolException("Wire v2 record contains unknown presence bits");
    }
    String recordTopic = reader.string();
    int recordPartition = reader.int32();
    if (!topic.equals(recordTopic) || partition != recordPartition) {
      throw new CursusProtocolException("Wire v2 record routing conflicts with batch");
    }
    long offset = reader.uint64();
    String payload = reader.string();
    long timestamp = (presence & RECORD_TIMESTAMP) != 0 ? reader.uint64() : 0;

    String producerId = null;
    long seqNum = 0;
    int epoch = 0;
    if ((presence & RECORD_PRODUCER) != 0) {
      producerId = reader.string();
      seqNum = reader.uint64();
      long wireEpoch = reader.uint64();
      if (wireEpoch < Integer.MIN_VALUE || wireEpoch > Integer.MAX_VALUE) {
        throw new CursusProtocolException("Wire v2 producer epoch is outside Java int range");
      }
      epoch = (int) wireEpoch;
    }
    String key = (presence & RECORD_KEY) != 0 ? reader.string() : null;
    String eventType = (presence & RECORD_EVENT_TYPE) != 0 ? reader.string() : null;
    long schemaVersion =
        (presence & RECORD_SCHEMA_VERSION) != 0 ? Integer.toUnsignedLong(reader.uint32()) : 0;
    long aggregateVersion = (presence & RECORD_AGGREGATE_VERSION) != 0 ? reader.uint64() : 0;
    String metadata = (presence & RECORD_METADATA) != 0 ? reader.string() : null;
    String transactionalId = (presence & (1L << 7)) != 0 ? reader.string() : null;
    String transactionState = (presence & (1L << 8)) != 0 ? reader.string() : null;
    String transactionMarker = (presence & (1L << 9)) != 0 ? reader.string() : null;
    String controlBatchType = (presence & (1L << 10)) != 0 ? reader.string() : null;
    int controlBatchVersion = (presence & (1L << 11)) != 0 ? reader.int16() : 0;
    long controlCoordinatorEpoch = (presence & (1L << 12)) != 0 ? reader.uint64() : 0;
    byte[] controlKey = (presence & (1L << 13)) != 0 ? reader.bytes() : null;
    byte[] controlValue = (presence & (1L << 14)) != 0 ? reader.bytes() : null;
    reader.finish();
    validateTransactionFields(transactionState, transactionMarker, controlBatchType);

    return CursusMessage.builder()
        .offset(offset)
        .seqNum(seqNum)
        .producerId(producerId)
        .key(key)
        .epoch(epoch)
        .payload(payload)
        .eventType(eventType)
        .schemaVersion(schemaVersion)
        .aggregateVersion(aggregateVersion)
        .metadata(metadata)
        .timestamp(timestamp)
        .transactionalId(transactionalId)
        .transactionState(transactionState)
        .transactionMarker(transactionMarker)
        .controlBatchType(controlBatchType)
        .controlBatchVersion(controlBatchVersion)
        .controlBatchCoordinatorEpoch(controlCoordinatorEpoch)
        .controlBatchKey(controlKey)
        .controlBatchValue(controlValue)
        .build();
  }

  private static void validateTransactionFields(String state, String marker, String controlType) {
    if (!(state == null
        || state.isEmpty()
        || "open".equals(state)
        || "committed".equals(state)
        || "aborted".equals(state))) {
      throw new CursusProtocolException("Invalid transaction state: " + state);
    }
    if (!(marker == null
        || marker.isEmpty()
        || "commit".equals(marker)
        || "abort".equals(marker))) {
      throw new CursusProtocolException("Invalid transaction marker: " + marker);
    }
    if (!(controlType == null || controlType.isEmpty() || "transaction".equals(controlType))) {
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

  private static final class BinaryReader {
    private final ByteBuffer data;

    BinaryReader(byte[] value) {
      if (value.length > WireProtocol.MAX_FRAME_PAYLOAD) {
        throw new CursusProtocolException("Wire v2 payload exceeds maximum frame size");
      }
      data = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
    }

    int uint16() {
      require(2);
      return Short.toUnsignedInt(data.getShort());
    }

    int int16() {
      require(2);
      return data.getShort();
    }

    int uint32() {
      require(4);
      return data.getInt();
    }

    int int32() {
      return uint32();
    }

    long uint64() {
      require(8);
      return data.getLong();
    }

    String string() {
      return new String(bytes(), StandardCharsets.UTF_8);
    }

    byte[] bytes() {
      int length = uint32();
      if (length < 0) {
        throw new CursusProtocolException("Wire v2 field length exceeds Java int range");
      }
      require(length);
      byte[] result = new byte[length];
      data.get(result);
      return result;
    }

    void finish() {
      if (data.hasRemaining()) {
        throw new CursusProtocolException("Wire v2 payload has trailing bytes");
      }
    }

    private void require(int size) {
      if (size < 0 || data.remaining() < size) {
        throw new CursusProtocolException("Truncated Wire v2 binary field");
      }
    }
  }
}
