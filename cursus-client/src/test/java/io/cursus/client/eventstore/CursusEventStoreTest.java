package io.cursus.client.eventstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
                    assertThat(readCommand(client)).startsWith("READ_STREAM ");
                    writeFrame(client, "{\"status\":\"OK\",\"snapshot\":null,\"count\":0}");
                    envelopeSent.countDown();
                    assertThat(probeSecondCommand.await(5, TimeUnit.SECONDS)).isTrue();

                    client.setSoTimeout(250);
                    boolean commandInterleaved;
                    try {
                      readCommand(client);
                      commandInterleaved = true;
                    } catch (SocketTimeoutException expected) {
                      commandInterleaved = false;
                    }

                    client.setSoTimeout(5000);
                    writeFrame(client, new byte[0]);
                    if (!commandInterleaved) {
                      assertThat(readCommand(client)).startsWith("STREAM_VERSION ");
                      writeFrame(client, "OK version=0");
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
                    initialRead = readCommand(first);
                    firstReadSeen.countDown();
                    assertThat(probeConcurrentCommand.await(5, TimeUnit.SECONDS)).isTrue();

                    first.setSoTimeout(250);
                    try {
                      readCommand(first);
                      commandInterleaved = true;
                    } catch (SocketTimeoutException expected) {
                      commandInterleaved = false;
                    }
                    writeFrame(
                        first, "{\"status\":\"ERROR\",\"error\":\"topic_not_found topic=orders\"}");
                  }

                  try (Socket retried = server.accept()) {
                    retried.setSoTimeout(5000);
                    String retryRead = readCommand(retried);
                    writeFrame(retried, "{\"status\":\"OK\",\"snapshot\":null,\"count\":0}");
                    writeFrame(retried, new byte[0]);
                    String version = readCommand(retried);
                    writeFrame(retried, "OK version=0");
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
                  assertThat(commands.get(0)).startsWith("READ_STREAM ");
                  assertThat(commands.get(1)).isEqualTo("false");
                  assertThat(commands.get(2)).startsWith("READ_STREAM ");
                  assertThat(commands.get(3)).startsWith("STREAM_VERSION ");
                });
      } finally {
        executor.shutdownNow();
      }
    }
  }

  private static String readCommand(Socket socket) throws IOException {
    DataInputStream in = new DataInputStream(socket.getInputStream());
    int length = in.readInt();
    byte[] payload = in.readNBytes(length);
    if (payload.length != length || length < 2) {
      throw new IOException("incomplete command frame");
    }
    return new String(payload, 2, length - 2, StandardCharsets.UTF_8);
  }

  private static void writeFrame(Socket socket, String payload) throws IOException {
    writeFrame(socket, payload.getBytes(StandardCharsets.UTF_8));
  }

  private static void writeFrame(Socket socket, byte[] payload) throws IOException {
    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
    out.writeInt(payload.length);
    out.write(payload);
    out.flush();
  }

  private static AppendResult parseAppendResponse(CursusEventStore store, String response)
      throws Exception {
    Method method = CursusEventStore.class.getDeclaredMethod("parseAppendResponse", String.class);
    method.setAccessible(true);
    return (AppendResult) method.invoke(store, response);
  }
}
