package io.cursus.client.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.cursus.client.admin.AdminClient;
import io.cursus.client.admin.AdminClient.AdminConfig;
import io.cursus.client.admin.AdminClient.TopicDefinitionPatch;
import io.cursus.client.config.Acks;
import io.cursus.client.config.ConsumerMode;
import io.cursus.client.config.CursusConsumerConfig;
import io.cursus.client.config.CursusProducerConfig;
import io.cursus.client.consumer.CursusConsumer;
import io.cursus.client.eventstore.CursusEventStore;
import io.cursus.client.eventstore.Event;
import io.cursus.client.producer.CursusProducer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@EnabledIfEnvironmentVariable(named = "CURSUS_E2E_BROKER", matches = ".+")
class SdkWireV2E2ETest {

  @ParameterizedTest
  @ValueSource(strings = {"POLLING", "POLLING", "POLLING", "STREAMING", "STREAMING", "STREAMING"})
  void idempotentPublishThenConsumeOverWireV2(String modeName) throws Exception {
    ConsumerMode mode = ConsumerMode.valueOf(modeName);
    String broker = System.getenv("CURSUS_E2E_BROKER");
    String topic = "java-wire-v2-" + UUID.randomUUID().toString().substring(0, 8);
    provisionTopic(broker, topic, false, true);
    try (CursusProducer producer =
        new CursusProducer(
            CursusProducerConfig.builder()
                .brokers(List.of(broker))
                .topic(topic)
                .partitions(1)
                .acks(Acks.ALL)
                .idempotent(true)
                .compressionType("gzip")
                .batchSize(3)
                .lingerMs(0)
                .build())) {
      producer.send("wire-v2-1");
      producer.send("wire-v2-2");
      producer.send("wire-v2-3");
      producer.flush();
      assertThat(producer.getUniqueAckCount()).isEqualTo(3);
    }

    List<String> received = new CopyOnWriteArrayList<>();
    CountDownLatch complete = new CountDownLatch(3);
    CursusConsumer consumer =
        new CursusConsumer(
            CursusConsumerConfig.builder()
                .brokers(List.of(broker))
                .topic(topic)
                .groupId("java-e2e-" + UUID.randomUUID().toString().substring(0, 8))
                .consumerMode(mode)
                .sessionTimeoutMs(2000)
                .immediateCommit(true)
                .build());
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> worker =
        executor.submit(
            () ->
                consumer.start(
                    message -> {
                      received.add(message.getPayload());
                      complete.countDown();
                    }));
    try {
      assertThat(complete.await(15, TimeUnit.SECONDS)).isTrue();
    } finally {
      consumer.close();
      worker.get(5, TimeUnit.SECONDS);
      executor.shutdownNow();
    }
    assertThat(received).startsWith("wire-v2-1", "wire-v2-2", "wire-v2-3");
  }

  @RepeatedTest(3)
  void eventStoreReadsEnvelopeAndBatchOnOneCorrelatedRequest() {
    String broker = System.getenv("CURSUS_E2E_BROKER");
    String topic = "java-es-wire-v2-" + UUID.randomUUID().toString().substring(0, 8);
    String key = "aggregate-" + UUID.randomUUID().toString().substring(0, 8);
    provisionTopic(broker, topic, true, false);

    try (CursusEventStore store = new CursusEventStore(broker, topic, "java-e2e")) {
      store.append(key, 1, Event.builder().type("Created").payload("{\"x\":1}").build());
      store.append(key, 2, Event.builder().type("Updated").payload("{\"x\":2}").build());

      var stream = store.readStream(key);

      assertThat(stream.getEvents()).hasSize(2);
      assertThat(stream.getEvents().get(0).getType()).isEqualTo("Created");
      assertThat(stream.getEvents().get(1).getType()).isEqualTo("Updated");
    }
  }

  @RepeatedTest(3)
  void zeroAckPublishCompletesWithoutWaitingForResponse() {
    String broker = System.getenv("CURSUS_E2E_BROKER");
    String topic = "java-no-ack-wire-v2-" + UUID.randomUUID().toString().substring(0, 8);
    provisionTopic(broker, topic, false, false);

    try (CursusProducer producer =
        new CursusProducer(
            CursusProducerConfig.builder()
                .brokers(List.of(broker))
                .topic(topic)
                .partitions(1)
                .acks(Acks.NONE)
                .batchSize(1)
                .lingerMs(0)
                .build())) {
      producer.send("fire-and-forget");
      producer.flush();
      assertThat(producer.getUniqueAckCount()).isZero();
    }
  }

  private static void provisionTopic(
      String broker, String topic, boolean eventSourcing, boolean idempotent) {
    AdminClient admin =
        new AdminClient(new AdminConfig(List.of(broker), 3, 100, 5000, "none", null, null));
    admin.createTopic(
        topic,
        new TopicDefinitionPatch(
            1, null, idempotent, eventSourcing, null, null, null, null, null, null, null));
  }
}
