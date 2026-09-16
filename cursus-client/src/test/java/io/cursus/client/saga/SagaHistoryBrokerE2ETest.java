package io.cursus.client.saga;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cursus.client.admin.AdminClient;
import io.cursus.client.admin.AdminClient.AdminConfig;
import io.cursus.client.admin.AdminClient.TopicDefinitionPatch;
import io.cursus.client.config.Acks;
import io.cursus.client.config.ConsumerMode;
import io.cursus.client.config.CursusConsumerConfig;
import io.cursus.client.config.CursusProducerConfig;
import io.cursus.client.consumer.CursusConsumer;
import io.cursus.client.producer.CursusProducer;
import io.cursus.client.saga.jdbc.JdbcHistoryOutboxPublisher;
import io.cursus.client.saga.jdbc.JdbcSagaTransaction;
import io.cursus.client.sagapg.PostgresSagaMigrations;
import io.cursus.client.sagapg.PostgresSagaTransaction;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.postgresql.ds.PGSimpleDataSource;

@EnabledIfEnvironmentVariable(named = "CURSUS_SAGA_POSTGRES_DSN", matches = ".+")
@EnabledIfEnvironmentVariable(named = "CURSUS_SAGA_BROKER_ADDR", matches = ".+")
class SagaHistoryBrokerE2ETest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void publishes_immutable_history_to_the_configured_cursus_observation_topic() throws Exception {
    String broker = System.getenv("CURSUS_SAGA_BROKER_ADDR");
    String topic = "java-saga-history-" + UUID.randomUUID().toString().substring(0, 8);
    new AdminClient(new AdminConfig(List.of(broker), 3, 100, 5000, "none", null, null))
        .createTopic(topic, new TopicDefinitionPatch(1, null, false, false, null, null, null, null, null, null, null));

    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setUrl(jdbcUrl(System.getenv("CURSUS_SAGA_POSTGRES_DSN")));
    PostgresSagaMigrations.migrate(dataSource);
    CountDownLatch complete = new CountDownLatch(5);
    List<Map<String, Object>> received = new ArrayList<>();
    CursusConsumer consumer =
        new CursusConsumer(
            CursusConsumerConfig.builder()
                .brokers(List.of(broker))
                .topic(topic)
                .groupId("java-saga-history-" + UUID.randomUUID().toString().substring(0, 8))
                .consumerMode(ConsumerMode.POLLING)
                .immediateCommit(true)
                .build());
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> worker =
        executor.submit(
            () ->
                consumer.start(
                    message -> {
                      try {
                        synchronized (received) {
                          received.add(JSON.readValue(message.getPayload(), new TypeReference<>() {}));
                        }
                        complete.countDown();
                      } catch (Exception error) {
                        throw new IllegalStateException(error);
                      }
                    }));
    try (CursusProducer producer =
        new CursusProducer(
            CursusProducerConfig.builder()
                .brokers(List.of(broker))
                .topic(topic)
                .partitions(1)
                .acks(Acks.ALL)
                .batchSize(1)
                .lingerMs(0)
                .build())) {
      String sagaId = "broker-e2e-" + UUID.randomUUID();
      TransactionalSagaManager manager =
          new TransactionalSagaManager(
              new SagaDefinition(
                  "broker-e2e",
                  Map.of(
                      "OrderCreated",
                      (state, event) -> {
                        state.setStatus(SagaState.WAITING);
                        state.setStepId("reserve");
                        return List.of(new SagaCommand("Reserve", "{}").effectId("reserve:1"));
                      })),
              new PostgresSagaTransaction(dataSource, topic),
              new SagaHistoryOptions("test", "orders"));
      manager.handle(new SagaEventEnvelope("event-" + sagaId, "OrderCreated", sagaId, "{}"));
      JdbcHistoryOutboxPublisher outbox =
          new JdbcHistoryOutboxPublisher(
              dataSource,
              new CursusHistoryPublisher(producer, topic),
              JdbcSagaTransaction.Dialect.POSTGRES);
      assertThat(outbox.publishPending(10)).isEqualTo(5);
      assertThat(complete.await(15, TimeUnit.SECONDS)).isTrue();
      assertThat(received).hasSize(5);
      assertThat(received).extracting(event -> event.get("event_type"))
          .containsExactlyInAnyOrder(
              "run.started", "step.started", "command.enqueued", "step.completed", "run.waiting");
      assertThat(received).extracting(event -> event.get("history_schema_version")).containsOnly(1);
      assertThat(received).extracting(event -> event.get("history_event_id")).doesNotHaveDuplicates();
      assertThat(received).extracting(event -> event.get("saga_id")).containsOnly(sagaId);
    } finally {
      consumer.close();
      worker.get(5, TimeUnit.SECONDS);
      executor.shutdownNow();
    }
  }

  private static String jdbcUrl(String dsn) {
    if (dsn.startsWith("jdbc:")) return dsn;
    URI uri = URI.create(dsn);
    String query = uri.getRawQuery() == null ? "" : uri.getRawQuery();
    if (uri.getUserInfo() != null) {
      String[] credentials = uri.getUserInfo().split(":", 2);
      query += (query.isEmpty() ? "" : "&") + "user=" + encode(credentials[0]);
      if (credentials.length == 2) query += "&password=" + encode(credentials[1]);
    }
    return "jdbc:"
        + uri.getScheme()
        + "://"
        + uri.getHost()
        + (uri.getPort() == -1 ? "" : ":" + uri.getPort())
        + uri.getRawPath()
        + (query.isEmpty() ? "" : "?" + query);
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
