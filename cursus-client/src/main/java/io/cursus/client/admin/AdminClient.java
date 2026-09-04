package io.cursus.client.admin;

import io.cursus.client.protocol.BrokerCommandClient;
import io.cursus.client.protocol.ProtocolDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Dedicated, authenticated client for privileged topic mutations. */
public final class AdminClient {
  private static final Pattern TOPIC = Pattern.compile("[A-Za-z0-9._-]+");
  private static final Pattern TOKEN = Pattern.compile("[^\\s,=]+");

  public enum TopicCleanupPolicy {
    DELETE("delete"),
    COMPACT("compact");

    private final String value;

    TopicCleanupPolicy(String value) {
      this.value = value;
    }

    public String value() {
      return value;
    }

    static TopicCleanupPolicy parse(String value) {
      return switch (value) {
        case "delete" -> DELETE;
        case "compact" -> COMPACT;
        default -> throw new IllegalArgumentException("invalid cleanup policy: " + value);
      };
    }
  }

  public record AdminConfig(
      List<String> brokers,
      int maxRetries,
      long retryBackoffMs,
      int requestTimeoutMs,
      String tlsCertPath,
      String tlsKeyPath,
      String compressionType,
      String principal,
      String authToken) {
    public AdminConfig(
        List<String> brokers,
        int maxRetries,
        long retryBackoffMs,
        int requestTimeoutMs,
        String compressionType,
        String principal,
        String authToken) {
      this(
          brokers,
          maxRetries,
          retryBackoffMs,
          requestTimeoutMs,
          null,
          null,
          compressionType,
          principal,
          authToken);
    }

    public AdminConfig {
      brokers = brokers == null ? List.of("localhost:9000") : List.copyOf(brokers);
      if (brokers.isEmpty() || brokers.stream().anyMatch(String::isBlank)) {
        throw new IllegalArgumentException("at least one non-empty broker is required");
      }
      if (maxRetries < 0 || retryBackoffMs < 0 || requestTimeoutMs <= 0) {
        throw new IllegalArgumentException("invalid admin retry or timeout configuration");
      }
      if ((principal == null || principal.isBlank())
          != (authToken == null || authToken.isBlank())) {
        throw new IllegalArgumentException("principal and authToken must be configured together");
      }
      compressionType = compressionType == null ? "none" : compressionType;
    }

    public static AdminConfig defaults() {
      return new AdminConfig(
          List.of("localhost:9000"), 3, 100, 5000, null, null, "none", null, null);
    }
  }

  public record TopicDefinitionPatch(
      Integer partitions,
      Integer replicationFactor,
      Boolean idempotent,
      Boolean eventSourcing,
      TopicCleanupPolicy cleanupPolicy,
      Integer retentionHours,
      Long retentionBytes,
      String partitioner,
      String authPolicy,
      List<String> readAcl,
      List<String> writeAcl) {}

  public record TopicDefinition(
      String topic,
      long revision,
      long lifecycleEpoch,
      int partitions,
      int replicationFactor,
      boolean idempotent,
      boolean eventSourcing,
      TopicCleanupPolicy cleanupPolicy,
      int retentionHours,
      long retentionBytes,
      String partitioner,
      String authPolicy,
      List<String> readAcl,
      List<String> writeAcl) {}

  public record DeleteTopicOptions(boolean ifExists) {}

  public record DeleteTopicResult(String topic, boolean deleted, boolean cleanupPending) {}

  public record TruncateTopicOptions(long expectedRevision) {}

  public record TruncateTopicResult(
      String topic,
      boolean truncated,
      long revision,
      long lifecycleEpoch,
      long leo,
      long hwm,
      boolean cleanupPending) {}

  @FunctionalInterface
  public interface AdminTransport {
    String send(String command, String operation, boolean retryAmbiguous);
  }

  private final AdminTransport transport;

  public AdminClient(AdminConfig config) {
    BrokerCommandClient client =
        new BrokerCommandClient(
            config.brokers(),
            config.requestTimeoutMs(),
            config.maxRetries(),
            config.retryBackoffMs(),
            config.tlsCertPath(),
            config.tlsKeyPath(),
            config.compressionType(),
            config.principal(),
            config.authToken());
    this.transport = client::sendAny;
  }

  public AdminClient(AdminTransport transport) {
    if (transport == null) throw new IllegalArgumentException("admin transport is required");
    this.transport = transport;
  }

  public TopicDefinition createTopic(String topic, TopicDefinitionPatch definition) {
    return applyTopicPatch(topic, definition);
  }

  public TopicDefinition updateTopic(String topic, TopicDefinitionPatch patch) {
    return applyTopicPatch(topic, patch);
  }

  public DeleteTopicResult deleteTopic(String topic, DeleteTopicOptions options) {
    String command = "DELETE topic=" + topic(topic);
    if (options.ifExists()) command += " if_exists=true";
    Map<String, String> fields =
        ProtocolDecoder.requireOk(
            transport.send(command, "delete topic", options.ifExists()), "delete topic");
    return new DeleteTopicResult(
        required(fields, "topic"),
        bool(fields, "deleted", null),
        bool(fields, "cleanup_pending", false));
  }

  public TruncateTopicResult truncateTopic(String topic, TruncateTopicOptions options) {
    if (options.expectedRevision() <= 0) {
      throw new IllegalArgumentException("expectedRevision must be positive");
    }
    String command =
        "TRUNCATE topic=" + topic(topic) + " expected_revision=" + options.expectedRevision();
    Map<String, String> fields =
        ProtocolDecoder.requireOk(
            transport.send(command, "truncate topic", false), "truncate topic");
    return new TruncateTopicResult(
        required(fields, "topic"),
        bool(fields, "truncated", null),
        number(fields, "revision"),
        number(fields, "lifecycle_epoch"),
        number(fields, "leo"),
        number(fields, "hwm"),
        bool(fields, "cleanup_pending", false));
  }

  private TopicDefinition applyTopicPatch(String topic, TopicDefinitionPatch patch) {
    List<String> parts = new ArrayList<>(List.of("CREATE", "topic=" + topic(topic)));
    positive(parts, "partitions", patch.partitions());
    positive(parts, "replication_factor", patch.replicationFactor());
    optionalBoolean(parts, "idempotent", patch.idempotent());
    optionalBoolean(parts, "event_sourcing", patch.eventSourcing());
    if (patch.cleanupPolicy() != null) {
      parts.add("cleanup_policy=" + patch.cleanupPolicy().value());
    }
    nonNegative(parts, "retention_hours", patch.retentionHours());
    nonNegative(parts, "retention_bytes", patch.retentionBytes());
    if (patch.partitioner() != null) {
      if (!Set.of("hash_key", "round_robin").contains(patch.partitioner())) {
        throw new IllegalArgumentException("invalid partitioner: " + patch.partitioner());
      }
      parts.add("partitioner=" + patch.partitioner());
    }
    if (patch.authPolicy() != null) {
      if (!Set.of("open", "deny_write", "deny_read", "acl").contains(patch.authPolicy())) {
        throw new IllegalArgumentException("invalid authPolicy: " + patch.authPolicy());
      }
      parts.add("auth_policy=" + patch.authPolicy());
    }
    acl(parts, "read_acl", patch.readAcl());
    acl(parts, "write_acl", patch.writeAcl());
    Map<String, String> fields =
        ProtocolDecoder.requireOk(
            transport.send(String.join(" ", parts), "create or update topic", true),
            "create or update topic");
    return new TopicDefinition(
        required(fields, "topic"),
        number(fields, "revision"),
        number(fields, "lifecycle_epoch"),
        Math.toIntExact(number(fields, "partitions")),
        Math.toIntExact(number(fields, "replication_factor")),
        bool(fields, "idempotent", null),
        bool(fields, "event_sourcing", null),
        TopicCleanupPolicy.parse(required(fields, "cleanup_policy")),
        Math.toIntExact(number(fields, "retention_hours")),
        number(fields, "retention_bytes"),
        required(fields, "partitioner"),
        required(fields, "auth_policy"),
        splitAcl(fields.get("read_acl")),
        splitAcl(fields.get("write_acl")));
  }

  private static String topic(String value) {
    if (value == null || !TOPIC.matcher(value).matches()) {
      throw new IllegalArgumentException("invalid topic name: " + value);
    }
    return value;
  }

  private static void positive(List<String> parts, String name, Number value) {
    if (value == null) return;
    if (value.longValue() <= 0) throw new IllegalArgumentException(name + " must be positive");
    parts.add(name + "=" + value);
  }

  private static void nonNegative(List<String> parts, String name, Number value) {
    if (value == null) return;
    if (value.longValue() < 0) {
      throw new IllegalArgumentException(name + " must be non-negative");
    }
    parts.add(name + "=" + value);
  }

  private static void optionalBoolean(List<String> parts, String name, Boolean value) {
    if (value != null) parts.add(name + "=" + value);
  }

  private static void acl(List<String> parts, String name, List<String> values) {
    if (values == null) return;
    if (values.stream().anyMatch(value -> value == null || !TOKEN.matcher(value).matches())) {
      throw new IllegalArgumentException("invalid " + name + " principal");
    }
    parts.add(name + "=" + String.join(",", values));
  }

  private static String required(Map<String, String> fields, String name) {
    String value = fields.get(name);
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("missing " + name + " in admin response");
    }
    return value;
  }

  private static long number(Map<String, String> fields, String name) {
    return Long.parseLong(required(fields, name));
  }

  private static boolean bool(Map<String, String> fields, String name, Boolean fallback) {
    String value = fields.get(name);
    if (value == null && fallback != null) return fallback;
    if (!"true".equals(value) && !"false".equals(value)) {
      throw new IllegalArgumentException("invalid " + name + " in admin response");
    }
    return Boolean.parseBoolean(value);
  }

  private static List<String> splitAcl(String value) {
    return value == null || value.isEmpty() ? List.of() : List.of(value.split(","));
  }
}
