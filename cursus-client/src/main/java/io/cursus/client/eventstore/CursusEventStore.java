package io.cursus.client.eventstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cursus.client.exception.CursusConnectionException;
import io.cursus.client.exception.CursusException;
import io.cursus.client.message.CursusMessage;
import io.cursus.client.protocol.ProtocolDecoder;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CursusEventStore implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(CursusEventStore.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String addr;
  private final String topic;
  private final String producerId;
  private Socket socket;

  public CursusEventStore(String addr, String topic, String producerId) {
    this.addr = addr;
    this.topic = topic;
    this.producerId = producerId;
  }

  private Socket getSocket() throws Exception {
    if (socket != null && !socket.isClosed()) return socket;
    String[] parts = addr.split(":");
    socket = new Socket(parts[0], Integer.parseInt(parts[1]));
    socket.setSoTimeout(10000);
    return socket;
  }

  private void resetSocket() {
    if (socket != null) {
      try {
        socket.close();
      } catch (Exception ignored) {
      }
      socket = null;
    }
  }

  private String sendCommand(String command) throws Exception {
    Socket s = getSocket();
    OutputStream out = s.getOutputStream();
    InputStream in = s.getInputStream();

    byte[] cmdBytes = command.getBytes(StandardCharsets.UTF_8);
    byte[] payload = new byte[2 + cmdBytes.length];
    System.arraycopy(cmdBytes, 0, payload, 2, cmdBytes.length);

    byte[] frame = new byte[4 + payload.length];
    frame[0] = (byte) (payload.length >> 24);
    frame[1] = (byte) (payload.length >> 16);
    frame[2] = (byte) (payload.length >> 8);
    frame[3] = (byte) (payload.length);
    System.arraycopy(payload, 0, frame, 4, payload.length);
    out.write(frame);
    out.flush();

    byte[] lenBuf = in.readNBytes(4);
    int respLen =
        ((lenBuf[0] & 0xFF) << 24)
            | ((lenBuf[1] & 0xFF) << 16)
            | ((lenBuf[2] & 0xFF) << 8)
            | (lenBuf[3] & 0xFF);
    byte[] resp = in.readNBytes(respLen);
    return new String(resp, StandardCharsets.UTF_8);
  }

  private byte[] readFrame() throws Exception {
    Socket s = getSocket();
    InputStream in = s.getInputStream();
    byte[] lenBuf = in.readNBytes(4);
    int respLen =
        ((lenBuf[0] & 0xFF) << 24)
            | ((lenBuf[1] & 0xFF) << 16)
            | ((lenBuf[2] & 0xFF) << 8)
            | (lenBuf[3] & 0xFF);
    return in.readNBytes(respLen);
  }

  public void createTopic(int partitions) {
    try {
      String resp =
          sendCommand(
              "CREATE topic=" + topic + " partitions=" + partitions + " event_sourcing=true");
      if (resp.startsWith("ERROR")) {
        throw new CursusException("createTopic: " + resp);
      }
      log.info("EventStore topic '{}' created with {} partitions", topic, partitions);
    } catch (CursusException e) {
      throw e;
    } catch (Exception e) {
      resetSocket();
      throw new CursusConnectionException("createTopic failed", e);
    }
  }

  public AppendResult append(String key, long expectedVersion, Event event) {
    try {
      int sv = event.getSchemaVersion() > 0 ? event.getSchemaVersion() : 1;
      StringBuilder cmd = new StringBuilder();
      cmd.append("APPEND_STREAM topic=")
          .append(topic)
          .append(" key=")
          .append(key)
          .append(" version=")
          .append(expectedVersion)
          .append(" event_type=")
          .append(event.getType())
          .append(" schema_version=")
          .append(sv)
          .append(" producerId=")
          .append(producerId);
      if (event.getMetadata() != null && !event.getMetadata().isEmpty()) {
        cmd.append(" metadata=").append(event.getMetadata());
      }
      cmd.append(" message=").append(event.getPayload());

      String resp = sendCommand(cmd.toString());
      if (resp.startsWith("ERROR")) {
        throw new CursusException("append: " + resp);
      }
      return parseAppendResponse(resp);
    } catch (CursusException e) {
      throw e;
    } catch (Exception e) {
      resetSocket();
      throw new CursusConnectionException("append failed", e);
    }
  }

  private AppendResult parseAppendResponse(String resp) {
    long version = 0, offset = 0;
    int partition = 0;
    for (String part : resp.split("\\s+")) {
      String[] kv = part.split("=", 2);
      if (kv.length != 2) continue;
      switch (kv[0]) {
        case "version" -> version = Long.parseLong(kv[1]);
        case "offset" -> offset = Long.parseLong(kv[1]);
        case "partition" -> partition = Integer.parseInt(kv[1]);
        default -> {}
      }
    }
    return new AppendResult(version, offset, partition);
  }

  public StreamData readStream(String key) {
    return readStream(key, 0);
  }

  public StreamData readStream(String key, long fromVersion) {
    try {
      String cmd = "READ_STREAM topic=" + topic + " key=" + key;
      if (fromVersion > 0) cmd += " from_version=" + fromVersion;

      Socket s = getSocket();
      OutputStream out = s.getOutputStream();

      byte[] cmdBytes = cmd.getBytes(StandardCharsets.UTF_8);
      byte[] payload = new byte[2 + cmdBytes.length];
      System.arraycopy(cmdBytes, 0, payload, 2, cmdBytes.length);
      byte[] frame = new byte[4 + payload.length];
      frame[0] = (byte) (payload.length >> 24);
      frame[1] = (byte) (payload.length >> 16);
      frame[2] = (byte) (payload.length >> 8);
      frame[3] = (byte) (payload.length);
      System.arraycopy(payload, 0, frame, 4, payload.length);
      out.write(frame);
      out.flush();

      // Frame 1: JSON envelope
      byte[] envData = readFrame();
      JsonNode envelope = MAPPER.readTree(envData);

      Snapshot snapshot = null;
      JsonNode snapNode = envelope.get("snapshot");
      if (snapNode != null && !snapNode.isNull()) {
        snapshot = new Snapshot(snapNode.get("version").asLong(), snapNode.get("payload").asText());
      }

      // Frame 2: Binary batch
      byte[] batchData = readFrame();
      List<StreamEvent> events = new ArrayList<>();
      if (batchData.length > 0) {
        List<CursusMessage> messages = ProtocolDecoder.decodeBatchMessages(batchData);
        for (CursusMessage m : messages) {
          events.add(
              new StreamEvent(
                  m.getAggregateVersion(), m.getOffset(),
                  m.getEventType(), (int) m.getSchemaVersion(),
                  m.getPayload(), m.getMetadata()));
        }
      }

      return new StreamData(snapshot, events);
    } catch (Exception e) {
      resetSocket();
      throw new CursusConnectionException("readStream failed", e);
    }
  }

  public void saveSnapshot(String key, long version, String payload) {
    try {
      String resp =
          sendCommand(
              "SAVE_SNAPSHOT topic="
                  + topic
                  + " key="
                  + key
                  + " version="
                  + version
                  + " message="
                  + payload);
      if (resp.startsWith("ERROR")) {
        throw new CursusException("saveSnapshot: " + resp);
      }
    } catch (CursusException e) {
      throw e;
    } catch (Exception e) {
      resetSocket();
      throw new CursusConnectionException("saveSnapshot failed", e);
    }
  }

  public Snapshot readSnapshot(String key) {
    try {
      String resp = sendCommand("READ_SNAPSHOT topic=" + topic + " key=" + key);
      if ("NULL".equals(resp) || resp.contains("NOT_FOUND")) return null;
      if (resp.startsWith("ERROR")) {
        throw new CursusException("readSnapshot: " + resp);
      }
      JsonNode obj = MAPPER.readTree(resp);
      return new Snapshot(obj.get("version").asLong(), obj.get("payload").asText());
    } catch (CursusException e) {
      throw e;
    } catch (Exception e) {
      resetSocket();
      throw new CursusConnectionException("readSnapshot failed", e);
    }
  }

  public long streamVersion(String key) {
    try {
      String resp = sendCommand("STREAM_VERSION topic=" + topic + " key=" + key);
      return Long.parseLong(resp.trim());
    } catch (NumberFormatException e) {
      throw new CursusException("streamVersion: invalid response");
    } catch (Exception e) {
      resetSocket();
      throw new CursusConnectionException("streamVersion failed", e);
    }
  }

  @Override
  public void close() {
    resetSocket();
  }
}
