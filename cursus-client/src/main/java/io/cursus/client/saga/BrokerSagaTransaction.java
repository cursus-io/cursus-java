package io.cursus.client.saga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cursus.client.transaction.TransactionalProducer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * DB-free Saga transaction boundary.
 *
 * <p>The caller recovers and serializes its run state from the public state event stream, then
 * passes one transition here. This class atomically appends that state event, publishes command and
 * history records, and acknowledges the consumed inbox offset through the Cursus broker
 * transaction. Configure the source {@code CursusConsumer} with {@code enableAutoCommit(false)}.
 */
public final class BrokerSagaTransaction {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final UUID NAMESPACE = UUID.fromString("2ce850f6-b151-5e5a-a160-6b8d82527d54");

  public record Topics(String inbox, String state, String commands, String history) {
    public Topics {
      for (String topic : new String[] {inbox, state, commands, history}) {
        if (topic == null || topic.isBlank() || topic.startsWith("__")) {
          throw new IllegalArgumentException("broker Saga topics must be public and non-empty");
        }
      }
    }

    public static Topics defaults() {
      return new Topics(
          "cursus.saga-inbox.v1",
          "cursus.saga-state.v1",
          "cursus.saga-commands.v1",
          "cursus.saga-history.v1");
    }
  }

  public record Config(String sagaType, String environmentId, String serviceName, Topics topics) {
    public Config {
      if (sagaType == null || sagaType.isBlank() || sagaType.matches(".*\\s.*")) {
        throw new IllegalArgumentException("sagaType is required and cannot contain whitespace");
      }
      if (environmentId == null
          || environmentId.isBlank()
          || serviceName == null
          || serviceName.isBlank()) {
        throw new IllegalArgumentException("environmentId and serviceName are required");
      }
      if (topics == null) topics = Topics.defaults();
    }
  }

  /** Consumer-assignment metadata required by SEND_OFFSETS_TO_TXN. */
  public record Input(
      String sagaId,
      String runId,
      String sourceTopic,
      int sourcePartition,
      long sourceOffset,
      String group,
      String member,
      int generation,
      SagaEventEnvelope event) {
    public Input {
      if (sagaId == null
          || sagaId.isBlank()
          || runId == null
          || runId.isBlank()
          || sagaId.matches(".*\\s.*")
          || runId.matches(".*\\s.*")) {
        throw new IllegalArgumentException("explicit sagaId and runId are required");
      }
      if (sourceTopic == null || sourceTopic.isBlank() || sourcePartition < 0 || sourceOffset < 0) {
        throw new IllegalArgumentException("input topic, partition, and offset are required");
      }
      if (group == null
          || group.isBlank()
          || member == null
          || member.isBlank()
          || generation < 0) {
        throw new IllegalArgumentException("active consumer membership is required");
      }
      if (event == null
          || event.eventId() == null
          || event.eventId().isBlank()
          || event.eventType() == null
          || event.eventType().isBlank()) {
        throw new IllegalArgumentException("source event identity is required");
      }
    }
  }

  /** A command executor deduplicates by commandId before performing its external effect. */
  public record CommandEnvelope(
      String commandId,
      String effectId,
      String commandType,
      String sagaType,
      String sagaId,
      String runId,
      String correlationId,
      String causationId,
      String payload) {
    public String toJson() {
      try {
        return JSON.writeValueAsString(
            Map.of(
                "schema_version", 1,
                "command_id", required(commandId),
                "effect_id", empty(effectId),
                "command_type", required(commandType),
                "saga_type", required(sagaType),
                "saga_id", required(sagaId),
                "run_id", required(runId),
                "correlation_id", empty(correlationId),
                "causation_id", empty(causationId),
                "payload", empty(payload)));
      } catch (JsonProcessingException exception) {
        throw new IllegalStateException("serialize broker Saga command", exception);
      }
    }
  }

  /** One already-serialized, version-checked state stream record. */
  public record Transition(
      String statePayload,
      long expectedStateVersion,
      List<CommandEnvelope> commands,
      List<SagaHistoryEvent> history) {
    public Transition {
      if (statePayload == null || statePayload.isBlank() || expectedStateVersion < 1) {
        throw new IllegalArgumentException(
            "statePayload and positive expectedStateVersion are required");
      }
      commands = commands == null ? List.of() : List.copyOf(commands);
      history = history == null ? List.of() : List.copyOf(history);
    }
  }

  @FunctionalInterface
  public interface ProducerFactory {
    TransactionalProducer create(String transactionalId);
  }

  private final Config config;
  private final ProducerFactory producers;

  public BrokerSagaTransaction(Config config, ProducerFactory producers) {
    this.config = config;
    if (producers == null) throw new IllegalArgumentException("producer factory is required");
    this.producers = producers;
  }

  public String streamKey(String sagaId, String runId) {
    return config.sagaType() + ":" + sagaId + ":" + runId;
  }

  /**
   * Applies a normal transition and commits the source offset in the same broker transaction.
   * Publishing a command emits only command.enqueued; business success/failure must be represented
   * by an explicit later state transition and history record.
   */
  public void apply(Input input, Transition transition) {
    TransactionalProducer producer = producers.create(transactionId(input, "apply"));
    boolean committed = false;
    try {
      producer.beginTransaction();
      String key = streamKey(input.sagaId(), input.runId());
      producer.appendStream(
          config.topics().state(),
          key,
          transition.expectedStateVersion(),
          transition.statePayload(),
          "saga.state.transitioned",
          1,
          "");
      for (CommandEnvelope command : transition.commands()) {
        producer.publish(config.topics().commands(), -1, command.toJson(), command.commandId());
      }
      for (SagaHistoryEvent event : transition.history()) {
        producer.publish(config.topics().history(), -1, event.toJson(), event.getHistoryEventId());
      }
      producer.sendOffsetsToTransaction(
          input.sourceTopic(),
          input.group(),
          input.member(),
          input.generation(),
          Map.of(input.sourcePartition(), input.sourceOffset() + 1));
      producer.commitTransaction();
      committed = true;
    } finally {
      if (!committed) {
        try {
          producer.abortTransaction();
        } catch (RuntimeException ignored) {
          // Preserve the original transition error; the broker will expire/fence the open
          // transaction.
        }
      }
      producer.close();
    }
  }

  /**
   * Acknowledge an already-recorded inbox event without emitting a second state or history event.
   */
  public void acknowledgeDuplicate(Input input) {
    TransactionalProducer producer = producers.create(transactionId(input, "duplicate"));
    boolean committed = false;
    try {
      producer.beginTransaction();
      producer.sendOffsetsToTransaction(
          input.sourceTopic(),
          input.group(),
          input.member(),
          input.generation(),
          Map.of(input.sourcePartition(), input.sourceOffset() + 1));
      producer.commitTransaction();
      committed = true;
    } finally {
      if (!committed) {
        try {
          producer.abortTransaction();
        } catch (RuntimeException ignored) {
        }
      }
      producer.close();
    }
  }

  /**
   * Persists a handler-failure state and history record without its source offset. This is the
   * required fresh transaction after the successful transition has rolled back, so delivery remains
   * retryable.
   */
  public void recordFailure(Input input, Transition transition) {
    TransactionalProducer producer = producers.create(transactionId(input, "failure"));
    boolean committed = false;
    try {
      producer.beginTransaction();
      producer.appendStream(
          config.topics().state(),
          streamKey(input.sagaId(), input.runId()),
          transition.expectedStateVersion(),
          transition.statePayload(),
          "saga.state.failed",
          1,
          "");
      for (SagaHistoryEvent event : transition.history()) {
        producer.publish(config.topics().history(), -1, event.toJson(), event.getHistoryEventId());
      }
      producer.commitTransaction();
      committed = true;
    } finally {
      if (!committed) {
        try {
          producer.abortTransaction();
        } catch (RuntimeException ignored) {
        }
      }
      producer.close();
    }
  }

  /** UUIDv5-compatible deterministic identity shared with the Go and Python runtimes. */
  public static UUID deterministicId(String... parts) {
    try {
      ByteBuffer namespace = ByteBuffer.allocate(16);
      namespace.putLong(NAMESPACE.getMostSignificantBits());
      namespace.putLong(NAMESPACE.getLeastSignificantBits());
      MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
      sha1.update(namespace.array());
      sha1.update(String.join("\0", parts).getBytes(StandardCharsets.UTF_8));
      byte[] hash = sha1.digest();
      hash[6] = (byte) ((hash[6] & 0x0f) | 0x50);
      hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);
      ByteBuffer value = ByteBuffer.wrap(hash);
      return new UUID(value.getLong(), value.getLong());
    } catch (Exception exception) {
      throw new IllegalStateException("create deterministic broker Saga UUID", exception);
    }
  }

  public SagaHistoryEvent history(
      Input input,
      long sequence,
      String eventType,
      String stepId,
      Integer attempt,
      SagaCommand command,
      String payload,
      String error) {
    Instant now = Instant.now();
    return SagaHistoryEvent.builder()
        .historyEventId(
            deterministicId(
                    "history",
                    config.sagaType(),
                    input.sagaId(),
                    input.runId(),
                    Long.toString(sequence))
                .toString())
        .environmentId(config.environmentId())
        .serviceName(config.serviceName())
        .sagaType(config.sagaType())
        .sagaId(input.sagaId())
        .runId(input.runId())
        .sequence(sequence)
        .eventType(eventType)
        .occurredAt(now)
        .recordedAt(now)
        .stepId(empty(stepId))
        .attempt(attempt)
        .commandId(command == null ? "" : command.getCommandId())
        .effectId(command == null ? "" : command.getEffectId())
        .sourceEventId(input.event().eventId())
        .correlationId(empty(input.event().correlationId()))
        .causationId(empty(input.event().causationId()))
        .sourceTopic(input.sourceTopic())
        .sourcePartition(input.sourcePartition())
        .sourceOffset(input.sourceOffset())
        .aggregateType(empty(input.event().aggregateType()))
        .aggregateId(empty(input.event().aggregateId()))
        .aggregateVersion(input.event().aggregateVersion())
        .payload(empty(payload))
        .error(empty(error))
        .build();
  }

  private String transactionId(Input input, String phase) {
    return "saga-"
        + deterministicId(
                "transaction",
                config.serviceName(),
                input.sourceTopic(),
                Integer.toString(input.sourcePartition()),
                Long.toString(input.sourceOffset()),
                phase)
            .toString();
  }

  private static String required(String value) {
    if (value == null || value.isBlank())
      throw new IllegalArgumentException("required broker Saga value is blank");
    return value;
  }

  private static String empty(String value) {
    return value == null ? "" : value;
  }
}
