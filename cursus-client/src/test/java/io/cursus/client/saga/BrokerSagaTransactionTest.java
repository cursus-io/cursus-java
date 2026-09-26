package io.cursus.client.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.cursus.client.transaction.TransactionalProducer;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class BrokerSagaTransactionTest {
  @Test
  void derivesStableUuidV5CompatibleIdsForRedelivery() {
    UUID first =
        BrokerSagaTransaction.deterministicId(
            "history", "orders", "order-42", "de94b8eb-50c4-4a35-b324-59b9318af658", "1");
    UUID second =
        BrokerSagaTransaction.deterministicId(
            "history", "orders", "order-42", "de94b8eb-50c4-4a35-b324-59b9318af658", "1");

    assertThat(first).isEqualTo(second);
    assertThat(first.version()).isEqualTo(5);
  }

  @Test
  void rejectsReservedSagaTopics() {
    assertThatThrownBy(
            () ->
                new BrokerSagaTransaction.Topics(
                    "__cursus", "saga-state", "saga-commands", "saga-history"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void scopesTransactionIdentityToSagaConsumerAndRun() {
    List<String> ids = new java.util.ArrayList<>();
    BrokerSagaTransaction transactionWithIds =
        new BrokerSagaTransaction(
            new BrokerSagaTransaction.Config(
                "orders", "test", "checkout", BrokerSagaTransaction.Topics.defaults()),
            id -> {
              ids.add(id);
              return new RecordingProducer(id);
            });

    transactionWithIds.apply(input("order-42", "run-1", "orders-workers"), transition());
    transactionWithIds.apply(input("order-42", "run-1", "billing-workers"), transition());
    transactionWithIds.apply(input("order-43", "run-2", "orders-workers"), transition());

    assertThat(ids).hasSize(3).doesNotHaveDuplicates();
  }

  @Test
  @SuppressWarnings("unchecked")
  void copiesNestedStateBeforeInvokingTheHandler() {
    SagaState original = new SagaState("order-42", "orders", "order-42");
    original.setRunId("run-1");
    original.setUpdatedAt(Instant.parse("2026-09-26T00:00:00Z"));
    original.setData(
        new LinkedHashMap<>(Map.of("nested", new LinkedHashMap<>(Map.of("value", 1)))));
    SagaState.EffectState effect = new SagaState.EffectState("effect-1", "reserve");
    effect.setAttempts(1);
    original.getEffects().put("effect-1", effect);

    SagaState transition = BrokerSagaRuntime.copyState(original);
    ((Map<String, Object>) transition.getData().get("nested")).put("value", 2);
    transition.getEffects().get("effect-1").setAttempts(2);

    assertThat(((Map<String, Object>) original.getData().get("nested")).get("value")).isEqualTo(1);
    assertThat(original.getEffects().get("effect-1").getAttempts()).isEqualTo(1);
  }

  private static BrokerSagaTransaction.Input input(String sagaId, String runId, String group) {
    return new BrokerSagaTransaction.Input(
        sagaId,
        runId,
        "orders",
        0,
        7,
        group,
        "member-1",
        1,
        new SagaEventEnvelope(
            "event-1", "order.created", "", "", "", "", null, null, "", "", null, ""));
  }

  private static BrokerSagaTransaction.Transition transition() {
    return new BrokerSagaTransaction.Transition("{}", 1, List.of(), List.of());
  }

  private static final class RecordingProducer extends TransactionalProducer {
    private RecordingProducer(String id) {
      super(id, List.of("unused:1"));
    }

    @Override
    public void beginTransaction() {}

    @Override
    public void appendStream(
        String topic,
        String key,
        long expectedVersion,
        String message,
        String eventType,
        int schemaVersion,
        String metadata) {}

    @Override
    public void sendOffsetsToTransaction(
        String topic, String group, String member, int generation, Map<Integer, Long> offsets) {}

    @Override
    public void commitTransaction() {}
  }
}
