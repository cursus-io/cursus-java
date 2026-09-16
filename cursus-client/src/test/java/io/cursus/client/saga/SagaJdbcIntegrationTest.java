package io.cursus.client.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.cj.jdbc.MysqlDataSource;
import io.cursus.client.saga.jdbc.JdbcHistoryOutboxPublisher;
import io.cursus.client.saga.jdbc.JdbcSagaTransaction;
import io.cursus.client.sagamysql.MySqlSagaMigrations;
import io.cursus.client.sagamysql.MySqlSagaTransaction;
import io.cursus.client.sagapg.PostgresSagaMigrations;
import io.cursus.client.sagapg.PostgresSagaTransaction;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.postgresql.ds.PGSimpleDataSource;

class SagaJdbcIntegrationTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  private static final class RecordingPublisher implements JdbcHistoryOutboxPublisher.Publisher {
    private int failures = 1;
    private final List<String> payloads = new ArrayList<>();

    @Override
    public void publish(String topic, String payload) {
      payloads.add(payload);
      if (failures-- > 0) throw new IllegalStateException("broker unavailable");
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "CURSUS_SAGA_POSTGRES_DSN", matches = ".+")
  void persistsPostgresHistoryAndOutboxAtomically() throws Exception {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setUrl(jdbcUrl(System.getenv("CURSUS_SAGA_POSTGRES_DSN")));
    verify(dataSource, true);
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "CURSUS_SAGA_MYSQL_DSN", matches = ".+")
  void persistsMySqlHistoryAndOutboxAtomically() throws Exception {
    MysqlDataSource dataSource = new MysqlDataSource();
    dataSource.setUrl(jdbcUrl(System.getenv("CURSUS_SAGA_MYSQL_DSN")));
    verify(dataSource, false);
  }

  private void verify(DataSource dataSource, boolean postgres) throws Exception {
    if (postgres) PostgresSagaMigrations.migrate(dataSource);
    else MySqlSagaMigrations.migrate(dataSource);
    String id = "java-contract-" + UUID.randomUUID();
    SagaDefinition definition =
        new SagaDefinition(
            "java-contract",
            java.util.Map.of(
                "OrderCreated",
                (state, event) -> {
                  state.setStatus(SagaState.WAITING);
                  state.setStepId("reserve");
                  return List.of(new SagaCommand("Reserve", "{}"));
                }));
    SagaContracts.Transaction transaction =
        postgres
            ? new PostgresSagaTransaction(dataSource, "observability.saga-history.v1")
            : new MySqlSagaTransaction(dataSource, "observability.saga-history.v1");
    new TransactionalSagaManager(definition, transaction, new SagaHistoryOptions("test", "orders"))
        .handle(new SagaEventEnvelope("event-" + id, "OrderCreated", id, "{}"));
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement();
        var rows =
            statement.executeQuery(
                "SELECT "
                    + "(SELECT count(*) FROM cursus_saga_state WHERE saga_id='"
                    + id
                    + "'),"
                    + "(SELECT count(*) FROM cursus_saga_inbox WHERE event_id='event-"
                    + id
                    + "'),"
                    + "(SELECT count(*) FROM cursus_saga_outbox WHERE saga_id='"
                    + id
                    + "'),"
                    + "(SELECT count(*) FROM cursus_saga_history WHERE saga_id='"
                    + id
                    + "'),"
                    + "(SELECT count(*) FROM cursus_saga_history_outbox o JOIN cursus_saga_history h "
                    + "ON h.history_event_id=o.history_event_id WHERE h.saga_id='"
                    + id
                    + "')")) {
      rows.next();
      assertThat(List.of(rows.getInt(1), rows.getInt(2), rows.getInt(3), rows.getInt(4), rows.getInt(5)))
          .containsExactly(1, 1, 1, 5, 5);
    }
    String runId;
    try (var connection = dataSource.getConnection();
        var statement =
            connection.prepareStatement(
                "SELECT run_id FROM cursus_saga_history WHERE saga_id=? ORDER BY sequence LIMIT 1")) {
      statement.setString(1, id);
      try (var rows = statement.executeQuery()) {
        rows.next();
        runId = rows.getString(1);
      }
    }
    String collision =
        "INSERT INTO cursus_saga_history "
            + "(history_event_id,history_schema_version,environment_id,service_name,saga_type,saga_id,"
            + "run_id,sequence,event_type,occurred_at,recorded_at) "
            + "VALUES (?,1,'test','orders','java-contract',?,?,1,'run.started',"
            + (postgres ? "NOW(),NOW())" : "UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))");
    try (var connection = dataSource.getConnection();
        var statement = connection.prepareStatement(collision)) {
      statement.setString(1, UUID.randomUUID().toString());
      statement.setString(2, id);
      statement.setString(3, runId);
      assertThatThrownBy(statement::executeUpdate).isInstanceOf(Exception.class);
    }

    RecordingPublisher publisher = new RecordingPublisher();
    JdbcHistoryOutboxPublisher worker =
        new JdbcHistoryOutboxPublisher(
            dataSource,
            publisher,
            postgres ? JdbcSagaTransaction.Dialect.POSTGRES : JdbcSagaTransaction.Dialect.MYSQL);
    assertThatThrownBy(() -> worker.publishPending(1)).hasMessageContaining("broker unavailable");
    String historyEventId = historyEventId(publisher.payloads.get(0));
    try (var connection = dataSource.getConnection();
        var statement =
            connection.prepareStatement(
                "UPDATE cursus_saga_history_outbox SET status='PUBLISHED' WHERE history_event_id IN "
                    + "(SELECT history_event_id FROM cursus_saga_history WHERE saga_id=?) "
                    + "AND history_event_id<>?")) {
      statement.setString(1, id);
      statement.setString(2, historyEventId);
      statement.executeUpdate();
    }
    assertThat(worker.publishPending(1)).isEqualTo(1);
    assertThat(historyEventId(publisher.payloads.get(1))).isEqualTo(historyEventId);
    try (var connection = dataSource.getConnection();
        var statement =
            connection.prepareStatement(
                "SELECT status,attempts FROM cursus_saga_history_outbox WHERE history_event_id=?")) {
      statement.setString(1, historyEventId);
      try (var rows = statement.executeQuery()) {
        rows.next();
        assertThat(rows.getString(1)).isEqualTo("PUBLISHED");
        assertThat(rows.getInt(2)).isEqualTo(2);
      }
    }
  }

  private static String historyEventId(String payload) throws Exception {
    Map<String, Object> event = JSON.readValue(payload, new TypeReference<>() {});
    return (String) event.get("history_event_id");
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
