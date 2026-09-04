package io.cursus.client.protocol;

import io.cursus.client.exception.CursusProtocolException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32C;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/** Canonical Cursus Wire v2 frame, negotiation, command, and error codec. */
public final class WireProtocol {

  public static final int PROTOCOL_VERSION = 2;
  public static final int HEADER_SIZE = 32;
  public static final int MAX_FRAME_PAYLOAD = 64 * 1024 * 1024;
  public static final int FRAME_MAGIC = 0x43525332;
  public static final int COMMAND_PAYLOAD_MAGIC = 0x43525132;
  public static final int COMMAND_PAYLOAD_VERSION = 2;
  public static final int BATCH_MAGIC = 0x43425632;
  public static final int BATCH_VERSION = 2;

  private static final int FLAG_COMPRESSION_EXPLICIT = 1;
  private static final int FLAG_COMPRESSION_SHIFT = 1;
  private static final int FLAG_COMPRESSION_MASK = 0x0E;
  private static final int VALID_FRAME_FLAGS = FLAG_COMPRESSION_EXPLICIT | FLAG_COMPRESSION_MASK;

  private WireProtocol() {}

  public enum Kind {
    NEGOTIATION_REQUEST(1),
    NEGOTIATION_RESPONSE(2),
    REQUEST(3),
    RESPONSE(4),
    STREAM(5);

    private final int value;

    Kind(int value) {
      this.value = value;
    }

    public int value() {
      return value;
    }

    static Kind from(int value) {
      return Arrays.stream(values())
          .filter(item -> item.value == value)
          .findFirst()
          .orElseThrow(() -> protocolError("invalid Wire v2 frame kind " + value));
    }

    boolean negotiation() {
      return this == NEGOTIATION_REQUEST || this == NEGOTIATION_RESPONSE;
    }
  }

  public enum Status {
    NONE(0),
    OK(1),
    ERROR(2),
    STREAM_END(3);

    private final int value;

    Status(int value) {
      this.value = value;
    }

    public int value() {
      return value;
    }

    static Status from(int value) {
      return Arrays.stream(values())
          .filter(item -> item.value == value)
          .findFirst()
          .orElseThrow(() -> protocolError("invalid Wire v2 frame status " + value));
    }
  }

  public enum Compression {
    NONE(0),
    GZIP(1),
    SNAPPY(2),
    LZ4(3);

    private final int value;

    Compression(int value) {
      this.value = value;
    }

    public int value() {
      return value;
    }

    static Compression from(int value) {
      return Arrays.stream(values())
          .filter(item -> item.value == value)
          .findFirst()
          .orElseThrow(() -> protocolError("unsupported Wire v2 compression " + value));
    }
  }

  public enum ErrorClass {
    VALIDATION(1, "validation"),
    AUTHORIZATION(2, "authorization"),
    ROUTING(3, "routing"),
    AVAILABILITY(4, "availability"),
    CONFLICT(5, "conflict"),
    FENCING(6, "fencing"),
    NOT_FOUND(7, "not_found"),
    INTERNAL(8, "internal");

    private final int value;
    private final String wireName;

    ErrorClass(int value, String wireName) {
      this.value = value;
      this.wireName = wireName;
    }

    public int value() {
      return value;
    }

    public String wireName() {
      return wireName;
    }

    static ErrorClass from(int value) {
      return Arrays.stream(values())
          .filter(item -> item.value == value)
          .findFirst()
          .orElseThrow(() -> protocolError("invalid Wire v2 error class " + value));
    }
  }

  public enum Command {
    UNKNOWN(0),
    AUTH(1),
    CREATE(2),
    ALTER_TOPIC_CONFIG(3),
    DELETE(4),
    TRUNCATE(5),
    LIST(6),
    LIST_CLUSTER(7),
    CLUSTER_STATUS(8),
    ELECT_LEADER(9),
    PUBLISH(10),
    CONSUME(11),
    STREAM(12),
    HELP(13),
    HEARTBEAT(14),
    JOIN_GROUP(15),
    SYNC_GROUP(16),
    LEAVE_GROUP(17),
    COMMIT_OFFSET(18),
    BATCH_COMMIT(19),
    REGISTER_GROUP(20),
    GROUP_STATUS(21),
    FETCH_OFFSET(22),
    LIST_GROUPS(23),
    LIST_OFFSETS(24),
    DESCRIBE(25),
    INIT_PRODUCER_ID(26),
    BEGIN_TXN(27),
    TXN_PUBLISH(28),
    SEND_OFFSETS_TO_TXN(29),
    END_TXN(30),
    TXN_STATUS(31),
    APPEND_STREAM(32),
    READ_STREAM(33),
    SAVE_SNAPSHOT(34),
    READ_SNAPSHOT(35),
    STREAM_VERSION(36),
    REPLICATE_MESSAGE(37),
    REPLICATE_SNAPSHOT(38),
    LIST_SNAPSHOTS(39),
    FETCH_SNAPSHOT(40),
    CATCHUP_SNAPSHOTS(41),
    FIND_COORDINATOR(42),
    RAFT_APPLY(43),
    METADATA(44),
    INTERNAL_BATCH(45),
    NEGOTIATE(46),
    EXIT(47),
    JOIN_CLUSTER(48),
    LEAVE_CLUSTER(49),
    HEARTBEAT_CLUSTER(50),
    REPLICA_CATCHUP(51);

    private final int value;

    Command(int value) {
      this.value = value;
    }

    public int value() {
      return value;
    }

    static Command from(int value) {
      return Arrays.stream(values())
          .filter(item -> item.value == value)
          .findFirst()
          .orElseThrow(() -> protocolError("unknown Wire v2 command " + value));
    }

    static Command parse(String value) {
      try {
        Command result = Command.valueOf(value.trim().toUpperCase());
        if (result == UNKNOWN) throw protocolError("unknown Wire v2 command " + value);
        return result;
      } catch (IllegalArgumentException exception) {
        throw protocolError("unknown Wire v2 command " + value, exception);
      }
    }
  }

  public record Frame(Kind kind, Command command, Status status, long requestId, byte[] payload) {
    public Frame {
      payload = payload.clone();
    }

    @Override
    public byte[] payload() {
      return payload.clone();
    }
  }

  public record Request(Command command, byte[] payload, boolean responseSuppressed) {
    public Request {
      payload = payload.clone();
    }

    @Override
    public byte[] payload() {
      return payload.clone();
    }
  }

  public record ErrorPayload(
      String code,
      ErrorClass errorClass,
      boolean retryable,
      String message,
      Map<String, String> fields) {
    public ErrorPayload {
      fields = Map.copyOf(fields);
    }
  }

  public static Compression compressionFromName(String value) {
    String normalized = value == null ? "none" : value.trim().toLowerCase();
    return switch (normalized) {
      case "", "none" -> Compression.NONE;
      case "gzip" -> Compression.GZIP;
      case "snappy" -> Compression.SNAPPY;
      case "lz4" -> Compression.LZ4;
      default -> throw protocolError("unsupported compression type: " + value);
    };
  }

  public static long crc32c(byte[] data) {
    CRC32C checksum = new CRC32C();
    checksum.update(data, 0, data.length);
    return checksum.getValue();
  }

  public static byte[] encodeFrame(Frame frame, Compression compression) {
    validateFrame(frame);
    byte[] decoded = frame.payload();
    if (decoded.length > MAX_FRAME_PAYLOAD) throw protocolError("Wire v2 payload too large");
    Compression selected = frame.kind().negotiation() ? Compression.NONE : compression;
    int flags =
        frame.kind().negotiation()
            ? 0
            : FLAG_COMPRESSION_EXPLICIT | (selected.value() << FLAG_COMPRESSION_SHIFT);
    byte[] encoded = compress(decoded, selected);
    if (encoded.length > MAX_FRAME_PAYLOAD) {
      throw protocolError("Wire v2 encoded payload too large");
    }
    ByteBuffer result =
        ByteBuffer.allocate(HEADER_SIZE + encoded.length).order(ByteOrder.BIG_ENDIAN);
    result.putInt(FRAME_MAGIC);
    result.putShort((short) PROTOCOL_VERSION);
    result.put((byte) frame.kind().value());
    result.put((byte) flags);
    result.putShort((short) frame.command().value());
    result.putShort((short) frame.status().value());
    result.putLong(frame.requestId());
    result.putInt(encoded.length);
    result.putInt(decoded.length);
    result.putInt((int) crc32c(encoded));
    result.put(encoded);
    return result.array();
  }

  public static Frame decodeFrame(byte[] encodedFrame, Compression compression) {
    if (encodedFrame.length < HEADER_SIZE) throw protocolError("Wire v2 header is truncated");
    ByteBuffer data = ByteBuffer.wrap(encodedFrame).order(ByteOrder.BIG_ENDIAN);
    if (data.getInt() != FRAME_MAGIC) throw protocolError("invalid Wire v2 frame magic");
    int version = Short.toUnsignedInt(data.getShort());
    if (version != PROTOCOL_VERSION) throw protocolError("unsupported Wire v2 version " + version);
    Kind kind = Kind.from(Byte.toUnsignedInt(data.get()));
    int flags = Byte.toUnsignedInt(data.get());
    Command command = Command.from(Short.toUnsignedInt(data.getShort()));
    Status status = Status.from(Short.toUnsignedInt(data.getShort()));
    long requestId = data.getLong();
    int encodedSize = data.getInt();
    int decodedSize = data.getInt();
    long checksum = Integer.toUnsignedLong(data.getInt());
    validateSize(encodedSize, decodedSize);
    if (encodedFrame.length != HEADER_SIZE + encodedSize) {
      throw protocolError("Wire v2 encoded length mismatch");
    }
    Compression selected;
    if (kind.negotiation()) {
      if (flags != 0) throw protocolError("negotiation frame must be uncompressed");
      selected = Compression.NONE;
    } else {
      if ((flags & ~VALID_FRAME_FLAGS) != 0 || (flags & FLAG_COMPRESSION_EXPLICIT) == 0) {
        throw protocolError("invalid Wire v2 compression flags");
      }
      selected = Compression.from((flags & FLAG_COMPRESSION_MASK) >> FLAG_COMPRESSION_SHIFT);
      if (selected != compression) throw protocolError("Wire v2 compression mismatch");
    }
    byte[] encoded = new byte[encodedSize];
    data.get(encoded);
    if (crc32c(encoded) != checksum) throw protocolError("Wire v2 checksum mismatch");
    Frame frame =
        new Frame(kind, command, status, requestId, decompress(encoded, selected, decodedSize));
    validateFrame(frame);
    return frame;
  }

  public static int encodedFrameSize(byte[] header) {
    if (header.length != HEADER_SIZE) throw protocolError("Wire v2 header is truncated");
    ByteBuffer data = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
    if (data.getInt() != FRAME_MAGIC) throw protocolError("invalid Wire v2 frame magic");
    int encodedSize = data.getInt(20);
    int decodedSize = data.getInt(24);
    validateSize(encodedSize, decodedSize);
    return encodedSize;
  }

  public static byte[] encodeNegotiationRequest(List<Compression> compressions) {
    if (compressions.isEmpty()
        || compressions.size() > 4
        || Set.copyOf(compressions).size() != compressions.size()) {
      throw protocolError("invalid compression preferences");
    }
    ByteBuffer result = ByteBuffer.allocate(6 + compressions.size()).order(ByteOrder.BIG_ENDIAN);
    result.putShort((short) PROTOCOL_VERSION);
    result.putShort((short) PROTOCOL_VERSION);
    result.putShort((short) compressions.size());
    for (Compression compression : compressions) result.put((byte) compression.value());
    return result.array();
  }

  public static Compression decodeNegotiationResponse(byte[] data) {
    if (data.length != 3) throw protocolError("invalid negotiation response length");
    ByteBuffer payload = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
    int version = Short.toUnsignedInt(payload.getShort());
    if (version != PROTOCOL_VERSION) {
      throw protocolError("unsupported negotiated version " + version);
    }
    return Compression.from(Byte.toUnsignedInt(payload.get()));
  }

  public static Request encodeRequest(byte[] applicationPayload) {
    if (isBatch(applicationPayload)) {
      return new Request(
          Command.PUBLISH,
          applicationPayload,
          batchAcknowledgements(applicationPayload).equals("0"));
    }
    String text = new String(applicationPayload, StandardCharsets.UTF_8).trim();
    ParsedCommand parsed = parseCommand(text);
    if (parsed.positionals().size() > 1024 || parsed.fields().size() > 1024) {
      throw protocolError("command argument count exceeds maximum 1024");
    }
    BinaryWriter writer = new BinaryWriter();
    writer.uint32(COMMAND_PAYLOAD_MAGIC);
    writer.uint16(COMMAND_PAYLOAD_VERSION);
    writer.uint16(parsed.positionals().size());
    for (String positional : parsed.positionals()) writer.string(positional);
    writer.uint16(parsed.fields().size());
    for (Map.Entry<String, String> field : parsed.fields().entrySet()) {
      writer.string(field.getKey());
      writer.string(field.getValue());
    }
    boolean suppressed =
        parsed.command() == Command.PUBLISH && "0".equals(parsed.fields().get("acks"));
    return new Request(parsed.command(), writer.toByteArray(), suppressed);
  }

  public static boolean isBatch(byte[] data) {
    return data.length >= 6
        && ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN).getInt() == BATCH_MAGIC
        && Short.toUnsignedInt(ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN).getShort(4))
            == BATCH_VERSION;
  }

  public static byte[] encodeError(ErrorPayload error) {
    if (error.code() == null || error.code().isEmpty() || error.fields().size() > 256) {
      throw protocolError("invalid Wire v2 error payload");
    }
    BinaryWriter writer = new BinaryWriter();
    writer.string(error.code());
    writer.uint8(error.errorClass().value());
    writer.uint8(error.retryable() ? 1 : 0);
    writer.string(error.message());
    writer.uint16(error.fields().size());
    List<String> keys = new ArrayList<>(error.fields().keySet());
    Collections.sort(keys);
    for (String key : keys) {
      if (key.isEmpty()) throw protocolError("error field name is empty");
      writer.string(key);
      writer.string(error.fields().get(key));
    }
    return writer.toByteArray();
  }

  public static ErrorPayload decodeError(byte[] data) {
    BinaryReader reader = new BinaryReader(data);
    String code = reader.string();
    ErrorClass errorClass = ErrorClass.from(reader.uint8());
    int retryable = reader.uint8();
    if (retryable > 1) throw protocolError("invalid Wire v2 retryable flag");
    String message = reader.string();
    int count = reader.uint16();
    Map<String, String> fields = new LinkedHashMap<>();
    for (int index = 0; index < count; index++) {
      String key = reader.string();
      String value = reader.string();
      if (key.isEmpty() || fields.putIfAbsent(key, value) != null) {
        throw protocolError("invalid duplicate Wire v2 error field");
      }
    }
    reader.finish();
    if (code.isEmpty()) throw protocolError("Wire v2 error code is empty");
    return new ErrorPayload(code, errorClass, retryable == 1, message, fields);
  }

  public static byte[] responsePayload(Frame frame) {
    if (frame.status() == Status.ERROR) return renderError(decodeError(frame.payload()));
    if (frame.status() != Status.OK && frame.status() != Status.STREAM_END) {
      throw protocolError("unexpected Wire v2 response status " + frame.status());
    }
    return frame.payload();
  }

  public static byte[] renderError(ErrorPayload error) {
    StringBuilder result =
        new StringBuilder("ERROR: ")
            .append(error.code())
            .append(" class=")
            .append(error.errorClass().wireName())
            .append(" retryable=")
            .append(error.retryable());
    List<String> keys = new ArrayList<>(error.fields().keySet());
    Collections.sort(keys);
    for (String key : keys) {
      String value = error.fields().get(key);
      result.append(' ').append(key).append('=').append(quoteIfNeeded(value));
    }
    if (!error.message().isEmpty()) result.append(' ').append(error.message());
    return result.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static String quoteIfNeeded(String value) {
    if (value.chars().noneMatch(Character::isWhitespace)) return value;
    return '"' + value.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
  }

  private static String batchAcknowledgements(byte[] data) {
    BinaryReader reader = new BinaryReader(data);
    reader.uint32();
    reader.uint16();
    reader.uint16();
    reader.string();
    reader.int32();
    return reader.string();
  }

  private static ParsedCommand parseCommand(String text) {
    if (text.isEmpty()) throw protocolError("command is empty");
    int separator = text.indexOf(' ');
    String name = separator < 0 ? text : text.substring(0, separator);
    String rest = separator < 0 ? "" : text.substring(separator + 1).trim();
    Command command = Command.parse(name);
    Map<String, String> trailing = new LinkedHashMap<>();
    int messagePosition = trailingFieldPosition(rest, "message");
    int payloadPosition = trailingFieldPosition(rest, "payload");
    if (messagePosition >= 0) {
      trailing.put("message", rest.substring(messagePosition + "message=".length()).trim());
      rest = rest.substring(0, messagePosition).trim();
      int metadataPosition = trailingFieldPosition(rest, "metadata");
      if (metadataPosition >= 0) {
        String message = trailing.remove("message");
        trailing.put("metadata", rest.substring(metadataPosition + "metadata=".length()).trim());
        trailing.put("message", message);
        rest = rest.substring(0, metadataPosition).trim();
      }
    } else if (payloadPosition >= 0) {
      trailing.put("payload", rest.substring(payloadPosition + "payload=".length()).trim());
      rest = rest.substring(0, payloadPosition).trim();
    }

    List<String> positionals = new ArrayList<>();
    Map<String, String> fields = new LinkedHashMap<>();
    if (!rest.isEmpty()) {
      for (String part : rest.split("\\s+")) {
        int equals = part.indexOf('=');
        if (equals < 0) {
          positionals.add(part);
          continue;
        }
        String key = part.substring(0, equals);
        String value = part.substring(equals + 1);
        if (!validFieldName(key) || fields.putIfAbsent(key, value) != null) {
          throw protocolError("invalid or duplicate command field " + key);
        }
      }
    }
    for (Map.Entry<String, String> field : trailing.entrySet()) {
      if (fields.putIfAbsent(field.getKey(), field.getValue()) != null) {
        throw protocolError("duplicate command field " + field.getKey());
      }
    }
    return new ParsedCommand(command, positionals, fields);
  }

  private static int trailingFieldPosition(String text, String field) {
    String marker = field + '=';
    int position = text.indexOf(marker);
    while (position >= 0) {
      if (position == 0 || Character.isWhitespace(text.charAt(position - 1))) return position;
      position = text.indexOf(marker, position + 1);
    }
    return -1;
  }

  private static boolean validFieldName(String value) {
    if (value.isEmpty() || value.charAt(0) < 'a' || value.charAt(0) > 'z') return false;
    for (int index = 1; index < value.length(); index++) {
      char character = value.charAt(index);
      if (character == '_'
          || Character.isDigit(character)
          || (character >= 'a' && character <= 'z')
          || (character >= 'A' && character <= 'Z')) continue;
      return false;
    }
    return true;
  }

  private static void validateFrame(Frame frame) {
    if (frame.command() == Command.UNKNOWN) throw protocolError("unknown Wire v2 command");
    if (frame.kind().negotiation() && frame.command() != Command.NEGOTIATE) {
      throw protocolError("negotiation frame requires NEGOTIATE command");
    }
    switch (frame.kind()) {
      case NEGOTIATION_REQUEST -> {
        if (frame.status() != Status.NONE) {
          throw protocolError("negotiation request status must be zero");
        }
      }
      case NEGOTIATION_RESPONSE, RESPONSE -> {
        if (!EnumSet.of(Status.OK, Status.ERROR).contains(frame.status())) {
          throw protocolError("response status must be OK or error");
        }
        if (frame.kind() == Kind.RESPONSE && frame.requestId() == 0) {
          throw protocolError("response request id is required");
        }
      }
      case REQUEST -> {
        if (frame.status() != Status.NONE || frame.requestId() == 0) {
          throw protocolError("request status must be zero and request id is required");
        }
      }
      case STREAM -> {
        if (frame.status() == Status.NONE || frame.requestId() == 0) {
          throw protocolError("invalid stream response");
        }
      }
    }
  }

  private static void validateSize(int encoded, int decoded) {
    if (encoded < 0 || decoded < 0 || encoded > MAX_FRAME_PAYLOAD || decoded > MAX_FRAME_PAYLOAD) {
      throw protocolError("Wire v2 frame exceeds size limit");
    }
  }

  private static byte[] compress(byte[] data, Compression compression) {
    if (compression == Compression.NONE) return data;
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      switch (compression) {
        case GZIP -> {
          try (GZIPOutputStream stream = new GZIPOutputStream(output)) {
            stream.write(data);
          }
        }
        case SNAPPY -> {
          try (SnappyOutputStream stream = new SnappyOutputStream(output)) {
            stream.write(data);
          }
        }
        case LZ4 -> {
          try (LZ4FrameOutputStream stream = new LZ4FrameOutputStream(output)) {
            stream.write(data);
          }
        }
        default ->
            throw protocolError("unsupported compression " + compression.name().toLowerCase());
      }
      return output.toByteArray();
    } catch (IOException exception) {
      throw protocolError(
          "Wire v2 " + compression.name().toLowerCase() + " compression failed", exception);
    }
  }

  private static byte[] decompress(byte[] data, Compression compression, int decodedSize) {
    byte[] decoded;
    if (compression == Compression.NONE) {
      decoded = data;
    } else {
      try (var stream =
          switch (compression) {
            case GZIP -> new GZIPInputStream(new ByteArrayInputStream(data));
            case SNAPPY -> new SnappyInputStream(new ByteArrayInputStream(data));
            case LZ4 -> new LZ4FrameInputStream(new ByteArrayInputStream(data));
            default ->
                throw protocolError("unsupported compression " + compression.name().toLowerCase());
          }) {
        decoded = stream.readNBytes(decodedSize + 1);
      } catch (IOException exception) {
        throw protocolError(
            "Wire v2 " + compression.name().toLowerCase() + " decompression failed", exception);
      }
    }
    if (decoded.length != decodedSize) throw protocolError("decoded payload length mismatch");
    return decoded;
  }

  private static CursusProtocolException protocolError(String message) {
    return new CursusProtocolException(message);
  }

  private static CursusProtocolException protocolError(String message, Throwable cause) {
    return new CursusProtocolException(message, cause);
  }

  private record ParsedCommand(
      Command command, List<String> positionals, Map<String, String> fields) {}

  private static final class BinaryWriter {
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final DataOutputStream data = new DataOutputStream(output);

    void uint8(int value) {
      write(() -> data.writeByte(value));
    }

    void uint16(int value) {
      write(() -> data.writeShort(value));
    }

    void uint32(int value) {
      write(() -> data.writeInt(value));
    }

    void string(String value) {
      bytes(value.getBytes(StandardCharsets.UTF_8));
    }

    void bytes(byte[] value) {
      uint32(value.length);
      write(() -> data.write(value));
    }

    byte[] toByteArray() {
      byte[] result = output.toByteArray();
      if (result.length > MAX_FRAME_PAYLOAD) throw protocolError("Wire v2 payload too large");
      return result;
    }

    private void write(IORunnable operation) {
      try {
        operation.run();
      } catch (IOException exception) {
        throw protocolError("encode Wire v2 payload", exception);
      }
    }
  }

  private static final class BinaryReader {
    private final ByteBuffer data;

    BinaryReader(byte[] value) {
      if (value.length > MAX_FRAME_PAYLOAD) throw protocolError("Wire v2 payload too large");
      data = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
    }

    int uint8() {
      require(1);
      return Byte.toUnsignedInt(data.get());
    }

    int uint16() {
      require(2);
      return Short.toUnsignedInt(data.getShort());
    }

    int uint32() {
      require(4);
      return data.getInt();
    }

    int int32() {
      require(4);
      return data.getInt();
    }

    String string() {
      return new String(bytes(), StandardCharsets.UTF_8);
    }

    byte[] bytes() {
      int length = uint32();
      if (length < 0) throw protocolError("Wire v2 field length exceeds int range");
      require(length);
      byte[] result = new byte[length];
      data.get(result);
      return result;
    }

    void finish() {
      if (data.hasRemaining()) throw protocolError("Wire v2 payload has trailing bytes");
    }

    private void require(int size) {
      if (size < 0 || data.remaining() < size) throw protocolError("truncated Wire v2 field");
    }
  }

  @FunctionalInterface
  private interface IORunnable {
    void run() throws IOException;
  }
}
