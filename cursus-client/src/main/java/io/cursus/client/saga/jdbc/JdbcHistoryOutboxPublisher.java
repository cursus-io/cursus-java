package io.cursus.client.saga.jdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.sql.DataSource;

/** Leased at-least-once history publisher; it never emits command.published. */
@SuppressFBWarnings(
    value = "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING",
    justification =
        "The SQL differs only by the fixed PostgreSQL/MySQL dialect; values remain bound through PreparedStatement parameters.")
public final class JdbcHistoryOutboxPublisher {
  @FunctionalInterface
  public interface Publisher {
    void publish(String topic, String payload) throws Exception;
  }

  private final DataSource ds;
  private final Publisher publisher;
  private final JdbcSagaTransaction.Dialect dialect;

  public JdbcHistoryOutboxPublisher(
      DataSource ds, Publisher publisher, JdbcSagaTransaction.Dialect dialect) {
    this.ds = ds;
    this.publisher = publisher;
    this.dialect = dialect;
  }

  public int publishPending(int limit) throws Exception {
    int n = 0;
    while (n < limit) {
      String[] e = claim();
      if (e == null) return n;
      try {
        publisher.publish(e[1], e[2]);
        update(
            "UPDATE cursus_saga_history_outbox SET status='PUBLISHED',published_at="
                + now()
                + ",last_error='',lease_expires_at=NULL WHERE history_event_id="
                + id(),
            e[0]);
        n++;
      } catch (Exception x) {
        update(
            "UPDATE cursus_saga_history_outbox SET status='PENDING',last_error=?,lease_expires_at=NULL WHERE history_event_id="
                + id(),
            x.getMessage(),
            e[0]);
        throw x;
      }
    }
    return n;
  }

  private String[] claim() throws Exception {
    try (Connection c = ds.getConnection()) {
      c.setAutoCommit(false);
      String select =
          dialect == JdbcSagaTransaction.Dialect.POSTGRES
              ? "SELECT history_event_id::text,topic_name,payload::text FROM cursus_saga_history_outbox WHERE status='PENDING' OR(status='PUBLISHING' AND lease_expires_at<NOW()) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1"
              : "SELECT history_event_id,topic_name,CAST(payload AS CHAR) FROM cursus_saga_history_outbox WHERE status='PENDING' OR(status='PUBLISHING' AND lease_expires_at<UTC_TIMESTAMP(6)) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1";
      try (var s = c.createStatement();
          ResultSet r = s.executeQuery(select)) {
        if (!r.next()) {
          c.rollback();
          return null;
        }
        String key = r.getString(1);
        try (PreparedStatement u =
            c.prepareStatement(
                "UPDATE cursus_saga_history_outbox SET status='PUBLISHING',attempts=attempts+1,last_error='',lease_expires_at="
                    + (dialect == JdbcSagaTransaction.Dialect.POSTGRES
                        ? "NOW()+INTERVAL '2 minutes'"
                        : "DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 2 MINUTE)")
                    + " WHERE history_event_id="
                    + id())) {
          u.setString(1, key);
          u.executeUpdate();
        }
        c.commit();
        return new String[] {key, r.getString(2), r.getString(3)};
      }
    }
  }

  private void update(String sql, Object... values) throws Exception {
    try (Connection c = ds.getConnection();
        PreparedStatement s = c.prepareStatement(sql)) {
      for (int i = 0; i < values.length; i++) s.setObject(i + 1, values[i]);
      s.executeUpdate();
    }
  }

  private String now() {
    return dialect == JdbcSagaTransaction.Dialect.POSTGRES ? "NOW()" : "UTC_TIMESTAMP(6)";
  }

  private String id() {
    return dialect == JdbcSagaTransaction.Dialect.POSTGRES ? "?::uuid" : "?";
  }
}
