package io.cursus.client.framework;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cursus.client.eventstore.Event;
import io.cursus.client.eventstore.StreamEvent;
import java.time.Instant;
import java.util.UUID;

/** Language-neutral event envelope stored as a Cursus event payload. */
public record EventEnvelope(
    String eventId,
    String eventType,
    int schemaVersion,
    String aggregateType,
    String aggregateId,
    long aggregateVersion,
    Instant occurredAt,
    String correlationId,
    String associationKey,
    String causationId,
    JsonNode payload) {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static EventEnvelope create(
      String aggregateType, String aggregateId, String eventType, Object payload) {
    if (blank(aggregateType) || blank(aggregateId) || blank(eventType)) {
      throw new IllegalArgumentException("event envelope identity is incomplete");
    }
    return new EventEnvelope(
        UUID.randomUUID().toString(),
        eventType,
        1,
        aggregateType,
        aggregateId,
        0,
        Instant.now(),
        "",
        "",
        "",
        MAPPER.valueToTree(payload));
  }

  public EventEnvelope withAggregateVersion(long version) {
    return new EventEnvelope(
        eventId,
        eventType,
        schemaVersion,
        aggregateType,
        aggregateId,
        version,
        occurredAt,
        correlationId,
        associationKey,
        causationId,
        payload);
  }

  public EventEnvelope withSchemaVersion(int version, JsonNode newPayload) {
    return new EventEnvelope(
        eventId,
        eventType,
        version,
        aggregateType,
        aggregateId,
        aggregateVersion,
        occurredAt,
        correlationId,
        associationKey,
        causationId,
        newPayload);
  }

  public void validate() {
    if (blank(eventId) || blank(eventType) || blank(aggregateType) || blank(aggregateId)) {
      throw new IllegalArgumentException("event envelope identity is incomplete");
    }
    if (schemaVersion <= 0)
      throw new IllegalArgumentException("event schema version must be positive");
    if (aggregateVersion <= 0) {
      throw new IllegalArgumentException("aggregate version must be positive");
    }
    if (payload == null || payload.isMissingNode()) {
      throw new IllegalArgumentException("event payload must not be empty");
    }
  }

  public String toJson() {
    validate();
    ObjectNode value = MAPPER.createObjectNode();
    value.put("event_id", eventId);
    value.put("event_type", eventType);
    value.put("schema_version", schemaVersion);
    value.put("aggregate_type", aggregateType);
    value.put("aggregate_id", aggregateId);
    value.put("aggregate_version", aggregateVersion);
    value.put("occurred_at", occurredAt.toString());
    if (!blank(correlationId)) value.put("correlation_id", correlationId);
    if (!blank(associationKey)) value.put("association_key", associationKey);
    if (!blank(causationId)) value.put("causation_id", causationId);
    value.set("payload", payload);
    return value.toString();
  }

  public Event toEvent() {
    return Event.builder().type(eventType).schemaVersion(schemaVersion).payload(toJson()).build();
  }

  public static EventEnvelope fromStreamEvent(StreamEvent raw) {
    try {
      JsonNode value = MAPPER.readTree(raw.getPayload());
      String eventType = value.path("event_type").asText(raw.getType());
      int schemaVersion = value.path("schema_version").asInt(raw.getSchemaVersion());
      long aggregateVersion = value.path("aggregate_version").asLong(raw.getVersion());
      if (!eventType.equals(raw.getType())) {
        throw new IllegalArgumentException("event envelope type does not match stream type");
      }
      if (schemaVersion != raw.getSchemaVersion()) {
        throw new IllegalArgumentException(
            "event envelope schema version does not match stream schema version");
      }
      if (aggregateVersion != raw.getVersion()) {
        throw new IllegalArgumentException("event envelope version does not match stream version");
      }
      EventEnvelope result =
          new EventEnvelope(
              value.path("event_id").asText(),
              eventType,
              schemaVersion,
              value.path("aggregate_type").asText(),
              value.path("aggregate_id").asText(),
              aggregateVersion,
              Instant.parse(value.path("occurred_at").asText()),
              value.path("correlation_id").asText(),
              value.path("association_key").asText(),
              value.path("causation_id").asText(),
              value.get("payload"));
      result.validate();
      return result;
    } catch (RuntimeException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new IllegalArgumentException(
          "decode event envelope at offset " + raw.getOffset(), exception);
    }
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
