package io.cursus.client.consumer;

/** Active consumer-group membership required by SEND_OFFSETS_TO_TXN. */
public record TransactionalOffsetMetadata(
    String topic, String group, String member, int generation) {}
