package io.cursus.client.connection;

import io.cursus.client.exception.CursusConnectionException;
import io.cursus.client.exception.CursusProtocolException;
import io.cursus.client.protocol.ProtocolDecoder;
import io.cursus.client.protocol.WireProtocol;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Small blocking Wire v2 connection used by the synchronous event-store API. */
public final class BlockingWireConnection implements AutoCloseable {

  private final Socket socket;
  private final InputStream input;
  private final OutputStream output;
  private final WireProtocol.Compression compression;
  private long nextRequestId = 1;

  public BlockingWireConnection(String address, int timeoutMs) {
    this(address, timeoutMs, "none");
  }

  public BlockingWireConnection(String address, int timeoutMs, String compressionType) {
    this(address, timeoutMs, compressionType, null, null);
  }

  public BlockingWireConnection(
      String address, int timeoutMs, String compressionType, String principal, String authToken) {
    try {
      if ((principal == null || principal.isBlank())
          != (authToken == null || authToken.isBlank())) {
        throw new IllegalArgumentException("principal and authToken must be configured together");
      }
      String[] parts = address.split(":");
      socket = new Socket(parts[0], Integer.parseInt(parts[1]));
      socket.setSoTimeout(timeoutMs);
      input = socket.getInputStream();
      output = socket.getOutputStream();
      compression = negotiate(WireProtocol.compressionFromName(compressionType));
      if (principal != null) {
        String response =
            new String(
                sendCommand("AUTH principal=" + principal + " token=" + authToken),
                StandardCharsets.UTF_8);
        ProtocolDecoder.requireOk(response, "authentication");
      }
    } catch (Exception exception) {
      throw new CursusConnectionException("Failed to establish Wire v2 connection", exception);
    }
  }

  public synchronized byte[] sendCommand(String command) {
    List<byte[]> responses = send(command, 1);
    return responses.isEmpty() ? new byte[0] : responses.get(0);
  }

  public synchronized List<byte[]> readStream(String command) {
    return send(command, 2);
  }

  private List<byte[]> send(String command, int responseCount) {
    try {
      WireProtocol.Request request =
          WireProtocol.encodeRequest(command.getBytes(StandardCharsets.UTF_8));
      long requestId = nextRequestId++;
      writeFrame(
          new WireProtocol.Frame(
              WireProtocol.Kind.REQUEST,
              request.command(),
              WireProtocol.Status.NONE,
              requestId,
              request.payload()),
          compression);
      if (request.responseSuppressed()) return List.of();

      List<byte[]> responses = new ArrayList<>(responseCount);
      for (int index = 0; index < responseCount; index++) {
        WireProtocol.Frame frame = readFrame(compression);
        if (frame.requestId() != requestId || frame.command() != request.command()) {
          throw new CursusProtocolException("Wire v2 blocking response correlation mismatch");
        }
        responses.add(WireProtocol.responsePayload(frame));
        if (frame.status() == WireProtocol.Status.ERROR
            || frame.status() == WireProtocol.Status.STREAM_END) break;
      }
      return responses;
    } catch (IOException exception) {
      throw new CursusConnectionException("Wire v2 request failed", exception);
    }
  }

  private WireProtocol.Compression negotiate(WireProtocol.Compression requested)
      throws IOException {
    List<WireProtocol.Compression> preferences =
        requested == WireProtocol.Compression.NONE
            ? List.of(WireProtocol.Compression.NONE)
            : List.of(requested, WireProtocol.Compression.NONE);
    writeFrame(
        new WireProtocol.Frame(
            WireProtocol.Kind.NEGOTIATION_REQUEST,
            WireProtocol.Command.NEGOTIATE,
            WireProtocol.Status.NONE,
            0,
            WireProtocol.encodeNegotiationRequest(preferences)),
        WireProtocol.Compression.NONE);
    WireProtocol.Frame response = readFrame(WireProtocol.Compression.NONE);
    if (response.kind() != WireProtocol.Kind.NEGOTIATION_RESPONSE
        || response.command() != WireProtocol.Command.NEGOTIATE
        || response.status() != WireProtocol.Status.OK
        || response.requestId() != 0) {
      throw new CursusProtocolException("Cursus broker rejected Wire v2 negotiation");
    }
    WireProtocol.Compression selected = WireProtocol.decodeNegotiationResponse(response.payload());
    if (!preferences.contains(selected)) {
      throw new CursusProtocolException("Broker selected unrequested Wire v2 compression");
    }
    return selected;
  }

  private void writeFrame(WireProtocol.Frame frame, WireProtocol.Compression selected)
      throws IOException {
    output.write(WireProtocol.encodeFrame(frame, selected));
    output.flush();
  }

  private WireProtocol.Frame readFrame(WireProtocol.Compression selected) throws IOException {
    byte[] header = readExactly(WireProtocol.HEADER_SIZE);
    int payloadSize = WireProtocol.encodedFrameSize(header);
    byte[] payload = readExactly(payloadSize);
    byte[] encoded = new byte[WireProtocol.HEADER_SIZE + payloadSize];
    System.arraycopy(header, 0, encoded, 0, header.length);
    System.arraycopy(payload, 0, encoded, header.length, payload.length);
    return WireProtocol.decodeFrame(encoded, selected);
  }

  private byte[] readExactly(int size) throws IOException {
    byte[] value = input.readNBytes(size);
    if (value.length != size) {
      throw new CursusConnectionException("Connection closed while reading Wire v2 frame");
    }
    return value;
  }

  @Override
  public void close() {
    try {
      socket.close();
    } catch (IOException exception) {
      throw new CursusConnectionException("Failed to close Wire v2 connection", exception);
    }
  }
}
