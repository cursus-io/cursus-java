package io.cursus.client.saga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Language-neutral, append-only Cursus Saga execution-history v1 record. */
public final class SagaHistoryEvent {
  public static final int SCHEMA_VERSION = 1;
  private static final ObjectMapper JSON = new ObjectMapper();

  private final int historySchemaVersion;
  private final String historyEventId;
  private final String environmentId;
  private final String serviceName;
  private final String sagaType;
  private final String sagaId;
  private final String runId;
  private final long sequence;
  private final String eventType;
  private final Instant occurredAt;
  private final Instant recordedAt;
  private final String stepId;
  private final Integer attempt;
  private final String commandId;
  private final String effectId;
  private final String sourceEventId;
  private final String correlationId;
  private final String causationId;
  private final String sourceTopic;
  private final Integer sourcePartition;
  private final Long sourceOffset;
  private final String aggregateType;
  private final String aggregateId;
  private final Long aggregateVersion;
  private final String payload;
  private final String error;

  private SagaHistoryEvent(Builder builder) {
    historySchemaVersion = builder.historySchemaVersion;
    historyEventId = builder.historyEventId;
    environmentId = builder.environmentId;
    serviceName = builder.serviceName;
    sagaType = builder.sagaType;
    sagaId = builder.sagaId;
    runId = builder.runId;
    sequence = builder.sequence;
    eventType = builder.eventType;
    occurredAt = builder.occurredAt;
    recordedAt = builder.recordedAt;
    stepId = builder.stepId;
    attempt = builder.attempt;
    commandId = builder.commandId;
    effectId = builder.effectId;
    sourceEventId = builder.sourceEventId;
    correlationId = builder.correlationId;
    causationId = builder.causationId;
    sourceTopic = builder.sourceTopic;
    sourcePartition = builder.sourcePartition;
    sourceOffset = builder.sourceOffset;
    aggregateType = builder.aggregateType;
    aggregateId = builder.aggregateId;
    aggregateVersion = builder.aggregateVersion;
    payload = builder.payload;
    error = builder.error;
    if (historySchemaVersion != SCHEMA_VERSION
        || sequence < 1
        || blank(environmentId)
        || blank(serviceName)
        || blank(sagaType)
        || blank(sagaId)
        || blank(runId)
        || blank(eventType)
        || occurredAt == null
        || recordedAt == null) {
      throw new IllegalArgumentException("invalid Saga history v1 identity");
    }
  }

  public Map<String, Object> toMap() {
    Map<String, Object> values = new LinkedHashMap<>();
    values.put("history_schema_version", historySchemaVersion);
    values.put("history_event_id", historyEventId);
    values.put("environment_id", environmentId);
    values.put("service_name", serviceName);
    values.put("saga_type", sagaType);
    values.put("saga_id", sagaId);
    values.put("run_id", runId);
    values.put("sequence", Long.toString(sequence));
    values.put("event_type", eventType);
    values.put("occurred_at", occurredAt.toString());
    values.put("recorded_at", recordedAt.toString());
    put(values, "step_id", stepId);
    put(values, "attempt", attempt);
    put(values, "command_id", commandId);
    put(values, "effect_id", effectId);
    put(values, "source_event_id", sourceEventId);
    put(values, "correlation_id", correlationId);
    put(values, "causation_id", causationId);
    put(values, "source_topic", sourceTopic);
    put(values, "source_partition", sourcePartition);
    if (sourceOffset != null) values.put("source_offset", Long.toUnsignedString(sourceOffset));
    put(values, "aggregate_type", aggregateType);
    put(values, "aggregate_id", aggregateId);
    if (aggregateVersion != null)
      values.put("aggregate_version", Long.toUnsignedString(aggregateVersion));
    put(values, "payload", payload);
    put(values, "error", error);
    return values;
  }

  public String toJson() {
    try {
      return JSON.writeValueAsString(toMap());
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("serialize Saga history", exception);
    }
  }

  public int getHistorySchemaVersion() {
    return historySchemaVersion;
  }

  public String getHistoryEventId() {
    return historyEventId;
  }

  public String getEnvironmentId() {
    return environmentId;
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getSagaType() {
    return sagaType;
  }

  public String getSagaId() {
    return sagaId;
  }

  public String getRunId() {
    return runId;
  }

  public long getSequence() {
    return sequence;
  }

  public String getEventType() {
    return eventType;
  }

  public Instant getOccurredAt() {
    return occurredAt;
  }

  public Instant getRecordedAt() {
    return recordedAt;
  }

  public String getStepId() {
    return stepId;
  }

  public Integer getAttempt() {
    return attempt;
  }

  public String getCommandId() {
    return commandId;
  }

  public String getEffectId() {
    return effectId;
  }

  public String getSourceEventId() {
    return sourceEventId;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  public String getCausationId() {
    return causationId;
  }

  public String getSourceTopic() {
    return sourceTopic;
  }

  public Integer getSourcePartition() {
    return sourcePartition;
  }

  public Long getSourceOffset() {
    return sourceOffset;
  }

  public String getAggregateType() {
    return aggregateType;
  }

  public String getAggregateId() {
    return aggregateId;
  }

  public Long getAggregateVersion() {
    return aggregateVersion;
  }

  public String getPayload() {
    return payload;
  }

  public String getError() {
    return error;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private int historySchemaVersion = SCHEMA_VERSION;
    private String historyEventId = UUID.randomUUID().toString();
    private String environmentId = "";
    private String serviceName = "";
    private String sagaType = "";
    private String sagaId = "";
    private String runId = "";
    private long sequence;
    private String eventType = "";
    private Instant occurredAt;
    private Instant recordedAt;
    private String stepId = "";
    private Integer attempt;
    private String commandId = "";
    private String effectId = "";
    private String sourceEventId = "";
    private String correlationId = "";
    private String causationId = "";
    private String sourceTopic = "";
    private Integer sourcePartition;
    private Long sourceOffset;
    private String aggregateType = "";
    private String aggregateId = "";
    private Long aggregateVersion;
    private String payload = "";
    private String error = "";

    public Builder historyEventId(String value) {
      historyEventId = value;
      return this;
    }

    public Builder environmentId(String value) {
      environmentId = value;
      return this;
    }

    public Builder serviceName(String value) {
      serviceName = value;
      return this;
    }

    public Builder sagaType(String value) {
      sagaType = value;
      return this;
    }

    public Builder sagaId(String value) {
      sagaId = value;
      return this;
    }

    public Builder runId(String value) {
      runId = value;
      return this;
    }

    public Builder sequence(long value) {
      sequence = value;
      return this;
    }

    public Builder eventType(String value) {
      eventType = value;
      return this;
    }

    public Builder occurredAt(Instant value) {
      occurredAt = value;
      return this;
    }

    public Builder recordedAt(Instant value) {
      recordedAt = value;
      return this;
    }

    public Builder stepId(String value) {
      stepId = value;
      return this;
    }

    public Builder attempt(Integer value) {
      attempt = value;
      return this;
    }

    public Builder commandId(String value) {
      commandId = value;
      return this;
    }

    public Builder effectId(String value) {
      effectId = value;
      return this;
    }

    public Builder sourceEventId(String value) {
      sourceEventId = value;
      return this;
    }

    public Builder correlationId(String value) {
      correlationId = value;
      return this;
    }

    public Builder causationId(String value) {
      causationId = value;
      return this;
    }

    public Builder sourceTopic(String value) {
      sourceTopic = value;
      return this;
    }

    public Builder sourcePartition(Integer value) {
      sourcePartition = value;
      return this;
    }

    public Builder sourceOffset(Long value) {
      sourceOffset = value;
      return this;
    }

    public Builder aggregateType(String value) {
      aggregateType = value;
      return this;
    }

    public Builder aggregateId(String value) {
      aggregateId = value;
      return this;
    }

    public Builder aggregateVersion(Long value) {
      aggregateVersion = value;
      return this;
    }

    public Builder payload(String value) {
      payload = value;
      return this;
    }

    public Builder error(String value) {
      error = value;
      return this;
    }

    public SagaHistoryEvent build() {
      return new SagaHistoryEvent(this);
    }
  }

  private static void put(Map<String, Object> values, String key, Object value) {
    if (value != null && !(value instanceof String string && string.isEmpty()))
      values.put(key, value);
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
