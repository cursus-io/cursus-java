package io.cursus.client.saga;

/** Source event metadata carried into immutable Saga history. */
public record SagaEventEnvelope(
    String eventId,
    String eventType,
    String associationKey,
    String correlationId,
    String causationId,
    String sourceTopic,
    Integer sourcePartition,
    Long sourceOffset,
    String aggregateType,
    String aggregateId,
    Long aggregateVersion,
    String payload) {
  public SagaEventEnvelope(
      String eventId, String eventType, String associationKey, String payload) {
    this(eventId, eventType, associationKey, "", "", "", null, null, "", "", null, payload);
  }
}
