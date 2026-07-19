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

  private final List<String> addrs;
  private String addr;
  private final String topic;
  private final String producerId;
  private Socket socket;

  public CursusEventStore(String addr, String topic, String producerId) {
    this(List.of(addr), topic, producerId);
  }

  public CursusEventStore(List<String> addrs, String topic, String producerId) {
    if (addrs == null || addrs.isEmpty()) {
      throw new IllegalArgumentException("at least one broker address is required");
    }
    this.addrs = List.copyOf(addrs);
    this.addr = this.addrs.get(0);
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

  private void switchAddr(String newAddr) {
    if (newAddr != null && !newAddr.isEmpty()) {
      addr = newAddr;
    }
    resetSocket();
  }

  private String sendCommandOnce(String command) throws Exception {
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

    byte[] resp = readFrameFrom(in);
    return new String(resp, StandardCharsets.UTF_8);
  }

  private String sendCommand(String command) throws Exception {
    return sendCommand(command, 6, true);
  }

  private String sendCommand(String command, int retries, boolean retryTopicErrors)
      throws Exception {
    String last = "";
    for (int attempt = 0; attempt <= retries; attempt++) {
      String resp = sendCommandOnce(command);
      last = resp;
      String leader = leaderFromError(resp);
      if (leader != null && attempt < retries) {
        switchAddr(leader);
        continue;
      }
      if (retryTopicErrors
          && resp.startsWith("ERROR:")
          && isRetryableTopicError(resp)
          && attempt < retries) {
        resetSocket();
        Thread.sleep(Math.min(50L * (attempt + 1), 500L));
        continue;
      }
      return resp;
    }
    return last;
  }

  private byte[] readFrame() throws Exception {
    return readFrameFrom(getSocket().getInputStream());
  }

  private byte[] readFrameFrom(InputStream in) throws Exception {
    byte[] lenBuf = in.readNBytes(4);
    if (lenBuf.length != 4) {
      throw new CursusConnectionException("Connection closed while reading frame length");
    }
    int respLen =
        ((lenBuf[0] & 0xFF) << 24)
            | ((lenBuf[1] & 0xFF) << 16)
            | ((lenBuf[2] & 0xFF) << 8)
            | (lenBuf[3] & 0xFF);
    return in.readNBytes(respLen);
  }

  private static String leaderFromError(String resp) {
    if (resp == null) return null;
    String marker = "NOT_LEADER LEADER_IS";
    int idx = resp.indexOf(marker);
    if (idx < 0) return null;
    String rest = resp.substring(idx + marker.length()).trim();
    if (rest.isEmpty()) return null;
    return rest.split("\\s+")[0];
  }

  private static boolean isRetryableTopicError(String resp) {
    String text = resp == null ? "" : resp.toLowerCase();
    return text.contains("topic_not_found")
        || text.contains("no_raft_leader")
        || text.contains("leader_not_available");
  }

  public synchronized void createTopic(int partitions) {
    try {
      String resp =
          sendCommand(
              "CREATE topic=" + topic + " partitions=" + partitions + " event_sourcing=true",
              6,
              true);
      if (resp.startsWith("ERROR:")) {
        throw new CursusException("createTopic: " + resp);
      }
      if (!resp.startsWith("OK")) {
        throw new CursusException("createTopic: unexpected response: " + resp);
      }
      log.info("EventStore topic '{}' created with {} partitions", topic, partitions);
    } catch (CursusException e) {
      throw e;
    } catch (Exception e) {
      resetSocket();
      throw new CursusConnectionException("createTopic failed", e);
    }
  }

  public synchronized AppendResult append(String key, long expectedVersion, Event event) {
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
      if (resp.startsWith("ERROR:")) {
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
    if (!resp.startsWith("OK")) {
      throw new CursusException("append: unexpected response: " + resp);
    }
    Long version = null;
    Long offset = null;
    Integer partition = null;
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
    if (version == null || offset == null || partition == null) {
      throw new CursusException("append: missing fields in response: " + resp);
    }
    return new AppendResult(version, offset, partition);
  }

  public synchronized StreamData readStream(String key) {
    return readStream(key, 0);
  }

  public synchronized StreamData readStream(String key, long fromVersion) {
    String cmd = "READ_STREAM topic=" + topic + " key=" + key;
    if (fromVersion > 0) cmd += " from_version=" + fromVersion;

    String lastError = "read stream failed";
    for (int attempt = 0; attempt <= 6; attempt++) {
      try {
        return readStreamOnce(cmd);
      } catch (CursusException e) {
        lastError = e.getMessage();
        String leader = leaderFromError(lastError);
        if (leader != null && attempt < 6) {
          switchAddr(leader);
          continue;
        }
        if (isRetryableTopicError(lastError) && attempt < 6) {
          resetSocket();
          sleepBackoff(attempt);
          continue;
        }
        throw e;
      } catch (Exception e) {
        resetSocket();
        throw new CursusConnectionException("readStream failed", e);
      }
    }
    throw new CursusException("readStream: " + lastError);
  }

  private StreamData readStreamOnce(String cmd) throws Exception {
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

    byte[] envData = readFrame();
    JsonNode envelope;
    try {
      envelope = MAPPER.readTree(envData);
    } catch (Exception e) {
      throw new CursusException("readStream: " + new String(envData, StandardCharsets.UTF_8), e);
    }
    String status = envelope.path("status").asText();
    if ("ERROR".equals(status)) {
      throw new CursusException(
          "readStream: " + envelope.path("error").asText("read stream failed"));
    }
    if (!"OK".equals(status)) {
      throw new CursusException("readStream: unexpected status: " + status);
    }

    Snapshot snapshot = null;
    JsonNode snapNode = envelope.get("snapshot");
    if (snapNode != null && !snapNode.isNull()) {
      snapshot = new Snapshot(snapNode.get("version").asLong(), snapNode.get("payload").asText());
    }

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
  }

  public synchronized void saveSnapshot(String key, long version, String payload) {
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
      if (resp.startsWith("ERROR:")) {
        throw new CursusException("saveSnapshot: " + resp);
      }
      if (!resp.startsWith("OK")) {
        throw new CursusException("saveSnapshot: unexpected response: " + resp);
      }
    } catch (CursusException e) {
      throw e;
    } catch (Exception e) {
      resetSocket();
      throw new CursusConnectionException("saveSnapshot failed", e);
    }
  }

  public synchronized Snapshot readSnapshot(String key) {
    try {
      String resp = sendCommand("READ_SNAPSHOT topic=" + topic + " key=" + key);
      String snapshotJson = ProtocolDecoder.decodeSnapshotResponse(resp);
      if (snapshotJson == null) return null;
      JsonNode obj = MAPPER.readTree(snapshotJson);
      return new Snapshot(obj.get("version").asLong(), obj.get("payload").asText());
    } catch (CursusException e) {
      throw e;
    } catch (Exception e) {
      resetSocket();
      throw new CursusConnectionException("readSnapshot failed", e);
    }
  }

  public synchronized long streamVersion(String key) {
    try {
      String resp = sendCommand("STREAM_VERSION topic=" + topic + " key=" + key);
      return ProtocolDecoder.decodeVersionResponse(resp);
    } catch (CursusException e) {
      throw e;
    } catch (NumberFormatException e) {
      throw new CursusException("streamVersion: invalid response");
    } catch (Exception e) {
      resetSocket();
      throw new CursusConnectionException("streamVersion failed", e);
    }
  }

  private static void sleepBackoff(int attempt) {
    try {
      Thread.sleep(Math.min(50L * (attempt + 1), 500L));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CursusConnectionException("Interrupted during event store retry", e);
    }
  }

  @Override
  public synchronized void close() {
    resetSocket();
  }
}
