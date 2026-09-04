package io.cursus.client.connection;

import static org.assertj.core.api.Assertions.assertThat;

import io.cursus.client.protocol.WireProtocol;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class WireConnectionIntegrationTest {

  @Test
  void negotiatesAndCorrelatesOutOfOrderResponses() throws Exception {
    try (ServerSocket server = new ServerSocket(0)) {
      ExecutorService executor = Executors.newSingleThreadExecutor();
      Future<?> broker = executor.submit(() -> runBroker(server));
      String address = "127.0.0.1:" + server.getLocalPort();

      try (ConnectionManager manager = new ConnectionManager(List.of(address), null, null, 30000)) {
        var first = manager.sendCommand("LIST topic=orders");
        var second = manager.sendCommand("METADATA topic=orders");

        assertThat(new String(first.get(5, TimeUnit.SECONDS), StandardCharsets.UTF_8))
            .isEqualTo("OK list");
        assertThat(new String(second.get(5, TimeUnit.SECONDS), StandardCharsets.UTF_8))
            .isEqualTo("OK metadata");
      }

      broker.get(5, TimeUnit.SECONDS);
      executor.shutdownNow();
    }
  }

  private static void runBroker(ServerSocket server) {
    try (Socket socket = server.accept()) {
      InputStream input = socket.getInputStream();
      OutputStream output = socket.getOutputStream();
      WireProtocol.Frame negotiation = readFrame(input, WireProtocol.Compression.NONE);
      assertThat(negotiation.kind()).isEqualTo(WireProtocol.Kind.NEGOTIATION_REQUEST);
      writeFrame(
          output,
          new WireProtocol.Frame(
              WireProtocol.Kind.NEGOTIATION_RESPONSE,
              WireProtocol.Command.NEGOTIATE,
              WireProtocol.Status.OK,
              negotiation.requestId(),
              new byte[] {0, 2, 0}),
          WireProtocol.Compression.NONE);

      WireProtocol.Frame first = readFrame(input, WireProtocol.Compression.NONE);
      WireProtocol.Frame second = readFrame(input, WireProtocol.Compression.NONE);
      assertThat(first.command()).isEqualTo(WireProtocol.Command.LIST);
      assertThat(second.command()).isEqualTo(WireProtocol.Command.METADATA);

      writeFrame(output, response(second, "OK metadata"), WireProtocol.Compression.NONE);
      writeFrame(output, response(first, "OK list"), WireProtocol.Compression.NONE);
    } catch (Exception exception) {
      throw new AssertionError(exception);
    }
  }

  private static WireProtocol.Frame response(WireProtocol.Frame request, String payload) {
    return new WireProtocol.Frame(
        WireProtocol.Kind.RESPONSE,
        request.command(),
        WireProtocol.Status.OK,
        request.requestId(),
        payload.getBytes(StandardCharsets.UTF_8));
  }

  private static WireProtocol.Frame readFrame(
      InputStream input, WireProtocol.Compression compression) throws Exception {
    byte[] header = input.readNBytes(WireProtocol.HEADER_SIZE);
    int payloadSize = WireProtocol.encodedFrameSize(header);
    byte[] payload = input.readNBytes(payloadSize);
    byte[] frame = new byte[header.length + payload.length];
    System.arraycopy(header, 0, frame, 0, header.length);
    System.arraycopy(payload, 0, frame, header.length, payload.length);
    return WireProtocol.decodeFrame(frame, compression);
  }

  private static void writeFrame(
      OutputStream output, WireProtocol.Frame frame, WireProtocol.Compression compression)
      throws Exception {
    output.write(WireProtocol.encodeFrame(frame, compression));
    output.flush();
  }
}
