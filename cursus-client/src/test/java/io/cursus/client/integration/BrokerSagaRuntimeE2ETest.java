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
import io.cursus.client.consumer.TransactionalOffsetMetadata;
import io.cursus.client.eventstore.CursusEventStore;
import io.cursus.client.message.CursusMessage;
import io.cursus.client.producer.CursusProducer;
import io.cursus.client.saga.BrokerSagaRuntime;
import io.cursus.client.saga.BrokerSagaTransaction;
import io.cursus.client.saga.SagaCommand;
import io.cursus.client.saga.SagaEventEnvelope;
import io.cursus.client.transaction.TransactionalProducer;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Proves one broker transaction contains state, command, history, and the source offset. */
@EnabledIfEnvironmentVariable(named = "CURSUS_E2E_BROKER", matches = ".+")
class BrokerSagaRuntimeE2ETest {
  @Test
  void commitsStateCommandHistoryAndSourceOffsetWithoutDuplicatingTheRun() throws Exception {
    String broker = System.getenv("CURSUS_E2E_BROKER");
    String suffix = UUID.randomUUID().toString().substring(0, 8);
    String sourceTopic = "java-saga-source-" + suffix;
    String stateTopic = "java-saga-state-" + suffix;
    String commandTopic = "java-saga-commands-" + suffix;
    String historyTopic = "java-saga-history-" + suffix;
    provisionTopic(broker, sourceTopic, false, true);
    provisionTopic(broker, stateTopic, true, true);
    provisionTopic(broker, commandTopic, false, true);
    provisionTopic(broker, historyTopic, false, true);

    String sagaId = "order-" + suffix;
    String runId = UUID.randomUUID().toString();
    String eventId = UUID.randomUUID().toString();
    String group = "java-saga-e2e-" + suffix;
    SagaEventEnvelope event =
        new SagaEventEnvelope(
            eventId,
            "order.created",
            sagaId,
            "corr-" + suffix,
            "",
            sourceTopic,
            0,
            0L,
            "order",
            sagaId,
            1L,
            "{\"order_id\":\"" + sagaId + "\"}");
    BrokerSagaTransaction.Config sagaConfig =
        new BrokerSagaTransaction.Config(
            "orders",
            "test",
            "java-e2e",
            new BrokerSagaTransaction.Topics(sourceTopic, stateTopic, commandTopic, historyTopic));
    BrokerSagaTransaction boundary =
        new BrokerSagaTransaction(sagaConfig, id -> new TransactionalProducer(id, List.of(broker)));

    try (CursusEventStore stateStore = new CursusEventStore(broker, stateTopic, "java-saga-e2e")) {
      BrokerSagaRuntime runtime = new BrokerSagaRuntime(boundary, sagaConfig, stateStore);
      CountDownLatch applied = new CountDownLatch(1);
      CursusConsumer sourceConsumer =
          new CursusConsumer(
              CursusConsumerConfig.builder()
                  .brokers(List.of(broker))
                  .topic(sourceTopic)
                  .groupId(group)
                  .consumerMode(ConsumerMode.POLLING)
                  .enableAutoCommit(false)
                  .sessionTimeoutMs(5000)
                  .build());
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        executor.submit(
            () ->
                sourceConsumer.start(
                    message -> {
                      try {
                        TransactionalOffsetMetadata membership =
                            sourceConsumer.transactionalOffsetMetadata();
                        BrokerSagaTransaction.Input input =
                            input(sagaId, runId, sourceTopic, message, membership, event);
                        runtime.handle(input, (state, ignored) -> transition());
                        runtime.handle(input, (state, ignored) -> transition());
                        applied.countDown();
                      } catch (Exception exception) {
                        throw new RuntimeException(exception);
                      }
                    }));
        waitForMembership(sourceConsumer);
        try (CursusProducer producer =
            new CursusProducer(
                CursusProducerConfig.builder()
                    .brokers(List.of(broker))
                    .topic(sourceTopic)
                    .partitions(1)
                    .acks(Acks.ALL)
                    .idempotent(true)
                    .batchSize(1)
                    .lingerMs(0)
                    .build())) {
          producer.send(event.payload());
          producer.flush();
        }
        assertThat(applied.await(15, TimeUnit.SECONDS)).isTrue();
      } finally {
        sourceConsumer.close();
        executor.shutdownNow();
      }

      assertThat(stateStore.readStream(runtime.streamKey(sagaId, runId)).getEvents()).hasSize(1);
    }

    assertThat(consume(broker, commandTopic, 1)).singleElement().satisfies(value -> assertThat(value).contains("command_id"));
    List<String> history = consume(broker, historyTopic, 3);
    assertThat(history).hasSize(3);
    assertThat(history)
        .anySatisfy(value -> assertThat(value).contains("run.started"))
        .anySatisfy(value -> assertThat(value).contains("step.completed"))
        .anySatisfy(value -> assertThat(value).contains("command.enqueued"));
  }

  private static BrokerSagaRuntime.TransitionResult transition() {
    return new BrokerSagaRuntime.TransitionResult(
        List.of(new SagaCommand("reserve.inventory", "{\"sku\":\"sku-1\"}")),
        List.of(new BrokerSagaRuntime.HistoryDraft("step.completed", "reserve", 1, null, "", "")));
  }

  private static BrokerSagaTransaction.Input input(
      String sagaId,
      String runId,
      String sourceTopic,
      CursusMessage message,
      TransactionalOffsetMetadata membership,
      SagaEventEnvelope event) {
    return new BrokerSagaTransaction.Input(
        sagaId,
        runId,
        sourceTopic,
        0,
        message.getOffset(),
        membership.group(),
        membership.member(),
        membership.generation(),
        event);
  }

  private static List<String> consume(String broker, String topic, int count) throws Exception {
    List<String> messages = new CopyOnWriteArrayList<>();
    CountDownLatch received = new CountDownLatch(count);
    CursusConsumer consumer =
        new CursusConsumer(
            CursusConsumerConfig.builder()
                .brokers(List.of(broker))
                .topic(topic)
                .groupId("java-saga-observe-" + UUID.randomUUID().toString().substring(0, 8))
                .consumerMode(ConsumerMode.POLLING)
                .sessionTimeoutMs(5000)
                .build());
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(
          () ->
              consumer.start(
                  message -> {
                    messages.add(message.getPayload());
                    received.countDown();
                  }));
      assertThat(received.await(15, TimeUnit.SECONDS)).isTrue();
      return messages;
    } finally {
      consumer.close();
      executor.shutdownNow();
    }
  }

  private static void waitForMembership(CursusConsumer consumer) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      try {
        consumer.transactionalOffsetMetadata();
        return;
      } catch (IllegalStateException ignored) {
        Thread.sleep(25);
      }
    }
    throw new AssertionError("source consumer did not obtain a broker group assignment");
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
