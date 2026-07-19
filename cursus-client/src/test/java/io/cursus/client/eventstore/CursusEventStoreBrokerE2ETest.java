package io.cursus.client.eventstore;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class CursusEventStoreBrokerE2ETest {

  @Test
  void sharedStoreKeepsConcurrentReadStreamFramesAligned() throws Exception {
    Assumptions.assumeTrue(
        "true".equalsIgnoreCase(System.getenv("CURSUS_E2E")), "enable with CURSUS_E2E=true");

    String topic = "java-request-serialization-" + System.currentTimeMillis();
    String key = "order-1";
    try (CursusEventStore store =
        new CursusEventStore("127.0.0.1:10000", topic, "java-request-serialization-e2e")) {
      store.createTopic(1);
      AppendResult appended =
          store.append(
              key,
              1,
              Event.builder().type("OrderCreated").payload("{\"orderId\":\"order-1\"}").build());
      assertThat(appended.getVersion()).isEqualTo(1);

      ExecutorService executor = Executors.newFixedThreadPool(8);
      try {
        List<Future<?>> requests = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
          requests.add(
              executor.submit(
                  () -> {
                    StreamData stream = store.readStream(key);
                    assertThat(stream.getEvents())
                        .singleElement()
                        .satisfies(
                            event -> {
                              assertThat(event.getVersion()).isEqualTo(1);
                              assertThat(event.getType()).isEqualTo("OrderCreated");
                            });
                  }));
          requests.add(executor.submit(() -> assertThat(store.streamVersion(key)).isEqualTo(1)));
        }
        for (Future<?> request : requests) {
          request.get(10, TimeUnit.SECONDS);
        }
      } finally {
        executor.shutdownNow();
      }
    }
  }
}
