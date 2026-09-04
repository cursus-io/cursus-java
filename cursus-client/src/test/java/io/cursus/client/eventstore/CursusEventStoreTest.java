package io.cursus.client.eventstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.cursus.client.protocol.WireProtocol;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CursusEventStoreTest {

  @Test
  void parseAppendResponseStrictContract() throws Exception {
    CursusEventStore store = new CursusEventStore("localhost:9000", "orders", "producer-1");

    AppendResult result = parseAppendResponse(store, "OK version=3 offset=42 partition=1");

    assertThat(result.getVersion()).isEqualTo(3);
    assertThat(result.getOffset()).isEqualTo(42);
    assertThat(result.getPartition()).isEqualTo(1);
  }

  @Test
  void parseAppendResponseRejectsNonContractResponses() throws Exception {
    CursusEventStore store = new CursusEventStore("localhost:9000", "orders", "producer-1");

    assertThatThrownBy(() -> parseAppendResponse(store, "OK version=3"))
        .isInstanceOf(InvocationTargetException.class)
        .hasRootCauseMessage("append: missing fields in response: OK version=3");
    assertThatThrownBy(() -> parseAppendResponse(store, "3"))
        .isInstanceOf(InvocationTargetException.class)
        .hasRootCauseMessage("append: unexpected response: 3");
  }

  @Test
  void serializesConcurrentRequestUntilReadStreamConsumesSecondFrame() throws Exception {
    try (ServerSocket server = new ServerSocket(0);
        CursusEventStore store =
            new CursusEventStore("127.0.0.1:" + server.getLocalPort(), "orders", "producer-1")) {
      ExecutorService executor = Executors.newFixedThreadPool(3);
      CountDownLatch envelopeSent = new CountDownLatch(1);
      CountDownLatch probeSecondCommand = new CountDownLatch(1);
      CountDownLatch competitorStarted = new CountDownLatch(1);
      try {
        Future<Boolean> broker =
            executor.submit(
                () -> {
                  try (Socket client = server.accept()) {
                    WireProtocol.Frame readRequest = negotiateAndReadRequest(client);
                    assertThat(readRequest.command()).isEqualTo(WireProtocol.Command.READ_STREAM);
                    writeResponse(
                        client,
                        readRequest,
                        WireProtocol.Status.OK,
                        "{\"status\":\"OK\",\"snapshot\":null,\"count\":0}"
                            .getBytes(StandardCharsets.UTF_8));
                    envelopeSent.countDown();
                    assertThat(probeSecondCommand.await(5, TimeUnit.SECONDS)).isTrue();

                    client.setSoTimeout(250);
                    boolean commandInterleaved;
                    try {
                      readRequest(client);
                      commandInterleaved = true;
                    } catch (SocketTimeoutException expected) {
                      commandInterleaved = false;
                    }

                    client.setSoTimeout(5000);
                    writeResponse(client, readRequest, WireProtocol.Status.STREAM_END, new byte[0]);
                    if (!commandInterleaved) {
                      WireProtocol.Frame versionRequest = readRequest(client);
                      assertThat(versionRequest.command())
                          .isEqualTo(WireProtocol.Command.STREAM_VERSION);
                      writeResponse(
                          client,
                          versionRequest,
                          WireProtocol.Status.OK,
                          "OK version=0".getBytes(StandardCharsets.UTF_8));
                    }
                    return commandInterleaved;
                  }
                });

        Future<StreamData> read = executor.submit(() -> store.readStream("order-1"));
        assertThat(envelopeSent.await(5, TimeUnit.SECONDS)).isTrue();
        Future<Long> version =
            executor.submit(
                () -> {
                  competitorStarted.countDown();
                  return store.streamVersion("order-1");
                });
        assertThat(competitorStarted.await(5, TimeUnit.SECONDS)).isTrue();
        probeSecondCommand.countDown();

        assertThat(read.get(5, TimeUnit.SECONDS).getEvents()).isEmpty();
        assertThat(version.get(5, TimeUnit.SECONDS)).isZero();
        assertThat(broker.get(5, TimeUnit.SECONDS)).isFalse();
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  void serializesConcurrentRequestAcrossReadStreamRetry() throws Exception {
    try (ServerSocket server = new ServerSocket(0);
        CursusEventStore store =
            new CursusEventStore("127.0.0.1:" + server.getLocalPort(), "orders", "producer-1")) {
      ExecutorService executor = Executors.newFixedThreadPool(3);
      CountDownLatch firstReadSeen = new CountDownLatch(1);
      CountDownLatch probeConcurrentCommand = new CountDownLatch(1);
      CountDownLatch competitorStarted = new CountDownLatch(1);
      try {
        Future<List<String>> broker =
            executor.submit(
                () -> {
                  boolean commandInterleaved;
                  String initialRead;
                  try (Socket first = server.accept()) {
                    WireProtocol.Frame initialRequest = negotiateAndReadRequest(first);
                    initialRead = initialRequest.command().name();
                    firstReadSeen.countDown();
                    assertThat(probeConcurrentCommand.await(5, TimeUnit.SECONDS)).isTrue();

                    first.setSoTimeout(250);
                    try {
                      readRequest(first);
                      commandInterleaved = true;
                    } catch (SocketTimeoutException expected) {
                      commandInterleaved = false;
                    }
                    writeError(first, initialRequest, "topic_not_found", "topic=orders");
                  }

                  try (Socket retried = server.accept()) {
                    retried.setSoTimeout(5000);
                    WireProtocol.Frame retryRequest = negotiateAndReadRequest(retried);
                    String retryRead = retryRequest.command().name();
                    writeResponse(
                        retried,
                        retryRequest,
                        WireProtocol.Status.OK,
                        "{\"status\":\"OK\",\"snapshot\":null,\"count\":0}"
                            .getBytes(StandardCharsets.UTF_8));
                    writeResponse(
                        retried, retryRequest, WireProtocol.Status.STREAM_END, new byte[0]);
                    WireProtocol.Frame versionRequest = readRequest(retried);
                    String version = versionRequest.command().name();
                    writeResponse(
                        retried,
                        versionRequest,
                        WireProtocol.Status.OK,
                        "OK version=0".getBytes(StandardCharsets.UTF_8));
                    return List.of(
                        initialRead, Boolean.toString(commandInterleaved), retryRead, version);
                  }
                });

        Future<StreamData> read = executor.submit(() -> store.readStream("order-1"));
        assertThat(firstReadSeen.await(5, TimeUnit.SECONDS)).isTrue();
        Future<Long> version =
            executor.submit(
                () -> {
                  competitorStarted.countDown();
                  return store.streamVersion("order-1");
                });
        assertThat(competitorStarted.await(5, TimeUnit.SECONDS)).isTrue();
        probeConcurrentCommand.countDown();

        assertThat(read.get(5, TimeUnit.SECONDS).getEvents()).isEmpty();
        assertThat(version.get(5, TimeUnit.SECONDS)).isZero();
        assertThat(broker.get(5, TimeUnit.SECONDS))
            .satisfies(
                commands -> {
                  assertThat(commands.get(0)).isEqualTo("READ_STREAM");
                  assertThat(commands.get(1)).isEqualTo("false");
                  assertThat(commands.get(2)).isEqualTo("READ_STREAM");
                  assertThat(commands.get(3)).isEqualTo("STREAM_VERSION");
                });
      } finally {
        executor.shutdownNow();
      }
    }
  }

  private static WireProtocol.Frame negotiateAndReadRequest(Socket socket) throws Exception {
    WireProtocol.Frame negotiation = readFrame(socket.getInputStream());
    assertThat(negotiation.kind()).isEqualTo(WireProtocol.Kind.NEGOTIATION_REQUEST);
    writeFrame(
        socket.getOutputStream(),
        new WireProtocol.Frame(
            WireProtocol.Kind.NEGOTIATION_RESPONSE,
            WireProtocol.Command.NEGOTIATE,
            WireProtocol.Status.OK,
            negotiation.requestId(),
            new byte[] {0, 2, 0}));
    return readRequest(socket);
  }

  private static WireProtocol.Frame readRequest(Socket socket) throws Exception {
    WireProtocol.Frame request = readFrame(socket.getInputStream());
    assertThat(request.kind()).isEqualTo(WireProtocol.Kind.REQUEST);
    return request;
  }

  private static WireProtocol.Frame readFrame(InputStream input) throws Exception {
    byte[] header = input.readNBytes(WireProtocol.HEADER_SIZE);
    int payloadSize = WireProtocol.encodedFrameSize(header);
    byte[] payload = input.readNBytes(payloadSize);
    byte[] encoded = new byte[header.length + payload.length];
    System.arraycopy(header, 0, encoded, 0, header.length);
    System.arraycopy(payload, 0, encoded, header.length, payload.length);
    return WireProtocol.decodeFrame(encoded, WireProtocol.Compression.NONE);
  }

  private static void writeResponse(
      Socket socket, WireProtocol.Frame request, WireProtocol.Status status, byte[] payload)
      throws Exception {
    writeFrame(
        socket.getOutputStream(),
        new WireProtocol.Frame(
            status == WireProtocol.Status.STREAM_END
                ? WireProtocol.Kind.STREAM
                : WireProtocol.Kind.RESPONSE,
            request.command(),
            status,
            request.requestId(),
            payload));
  }

  private static void writeError(
      Socket socket, WireProtocol.Frame request, String code, String message) throws Exception {
    byte[] payload =
        WireProtocol.encodeError(
            new WireProtocol.ErrorPayload(
                code, WireProtocol.ErrorClass.NOT_FOUND, true, message, Map.of()));
    writeResponse(socket, request, WireProtocol.Status.ERROR, payload);
  }

  private static void writeFrame(OutputStream output, WireProtocol.Frame frame) throws Exception {
    output.write(WireProtocol.encodeFrame(frame, WireProtocol.Compression.NONE));
    output.flush();
  }

  private static AppendResult parseAppendResponse(CursusEventStore store, String response)
      throws Exception {
    Method method = CursusEventStore.class.getDeclaredMethod("parseAppendResponse", String.class);
    method.setAccessible(true);
    return (AppendResult) method.invoke(store, response);
  }
}
