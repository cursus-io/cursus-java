package io.cursus.client.saga;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class SagaHistoryEventTest {
  @Test
  void serializesWithGoV1FixtureSemantics() throws Exception {
    Map<String, Object> expected;
    try (InputStream input =
        Objects.requireNonNull(
            getClass().getResourceAsStream("/fixtures/saga-history-v1.json"),
            "Saga history fixture must be present")) {
      expected = new ObjectMapper().readValue(input, new TypeReference<>() {});
    }
    SagaHistoryEvent actual =
        SagaHistoryEvent.builder()
            .historyEventId("a4c9f9a0-291e-41e5-babb-3c13b84c4bb4")
            .environmentId("development")
            .serviceName("orders")
            .sagaType("order-fulfillment")
            .sagaId("order-42")
            .runId("de94b8eb-50c4-4a35-b324-59b9318af658")
            .sequence(3)
            .eventType("command.enqueued")
            .occurredAt(Instant.parse("2026-09-15T00:00:00Z"))
            .recordedAt(Instant.parse("2026-09-15T00:00:00Z"))
            .stepId("reserve-inventory")
            .attempt(1)
            .commandId("reserve-42")
            .effectId("reserve-inventory:1")
            .sourceEventId("event-order-42")
            .correlationId("order-42")
            .sourceTopic("orders.events")
            .sourcePartition(2)
            .sourceOffset(9007199254740993L)
            .aggregateType("order")
            .aggregateId("order-42")
            .aggregateVersion(7L)
            .payload("{\"order_id\":\"order-42\"}")
            .build();
    assertThat(actual.toMap()).isEqualTo(expected);
  }
}
