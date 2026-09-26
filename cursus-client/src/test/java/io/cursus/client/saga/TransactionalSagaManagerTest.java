package io.cursus.client.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.cursus.client.saga.SagaContracts.Stores;
import io.cursus.client.saga.SagaContracts.Transaction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TransactionalSagaManagerTest {
  private static final class MemoryTransaction implements Transaction {
    final Set<String> inbox = new HashSet<>();
    final Map<String, SagaState> states = new HashMap<>();
    final List<SagaCommand> commands = new ArrayList<>();
    final List<SagaHistoryEvent> history = new ArrayList<>();
    final List<String> failed = new ArrayList<>();

    @Override
    public <T> T run(SagaContracts.Operation<T> operation) throws Exception {
      return operation.apply(
          new Stores(
              new SagaContracts.InboxStore() {
                @Override
                public boolean claim(String consumer, String eventId) {
                  return inbox.add(consumer + ":" + eventId);
                }

                @Override
                public void complete(String consumer, String eventId) {}

                @Override
                public void fail(String consumer, String eventId, Exception cause) {
                  failed.add(consumer + ":" + eventId + ":" + cause.getMessage());
                }
              },
              new SagaContracts.StateStore() {
                @Override
                public SagaState loadForUpdate(String type, String id) {
                  return states.get(type + ":" + id);
                }

                @Override
                public void save(SagaState state) {
                  states.put(state.getSagaType() + ":" + state.getSagaId(), state);
                }
              },
              command -> commands.add(command), history::add));
    }
  }

  private static TransactionalSagaManager manager(
      MemoryTransaction transaction, Map<String, SagaDefinition.Handler> handlers) {
    return new TransactionalSagaManager(
        new SagaDefinition("orders", handlers),
        transaction,
        new SagaHistoryOptions("test", "orders"));
  }

  private static SagaEventEnvelope event(String id, String type) {
    return new SagaEventEnvelope(id, type, "order-42", "{\"order\":\"42\"}");
  }

  @Test
  void recordsOrderedRunAndNeverTreatsEnqueueOrPublishAsBusinessSuccess() throws Exception {
    MemoryTransaction transaction = new MemoryTransaction();
    TransactionalSagaManager manager =
        manager(
            transaction,
            Map.of(
                "OrderCreated",
                (state, ignored) -> {
                  state.setStatus(SagaState.WAITING);
                  state.setStepId("reserve");
                  return List.of(new SagaCommand("Reserve", "{}").effectId("reserve:1"));
                }));

    manager.handle(event("event-1", "OrderCreated"));
    manager.handle(event("event-1", "OrderCreated"));
    manager.recordCommandPublished("order-42", "reserve:1");

    assertThat(transaction.history)
        .extracting(SagaHistoryEvent::getEventType)
        .containsExactly(
            TransactionalSagaManager.RUN_STARTED,
            TransactionalSagaManager.STEP_STARTED,
            TransactionalSagaManager.COMMAND_ENQUEUED,
            TransactionalSagaManager.STEP_COMPLETED,
            TransactionalSagaManager.RUN_WAITING,
            TransactionalSagaManager.COMMAND_PUBLISHED);
    assertThat(transaction.history)
        .extracting(SagaHistoryEvent::getSequence)
        .containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
    assertThat(transaction.commands).hasSize(1);
    assertThat(transaction.states.get("orders:order-42").getEffects().get("reserve:1").getStatus())
        .isEqualTo(SagaState.PENDING);
  }

  @Test
  void records_failed_effect_and_compensation_terminal_outcome() throws Exception {
    MemoryTransaction transaction = new MemoryTransaction();
    TransactionalSagaManager manager =
        manager(
            transaction,
            Map.of(
                "OrderCreated",
                (state, ignored) -> {
                  state.setStatus(SagaState.WAITING);
                  return List.of(new SagaCommand("Reserve", "{}").effectId("reserve:1"));
                }));

    manager.handle(event("event-1", "OrderCreated"));
    manager.recordEffectResult("order-42", "reserve:1", false, new RuntimeException("declined"));
    manager.startCompensation("order-42", "release", new RuntimeException("declined"));
    manager.failCompensation("order-42", new RuntimeException("release failed"));

    assertThat(transaction.history)
        .extracting(SagaHistoryEvent::getEventType)
        .endsWith(
            TransactionalSagaManager.COMMAND_FAILED,
            TransactionalSagaManager.COMPENSATION_STARTED,
            TransactionalSagaManager.COMPENSATION_FAILED,
            TransactionalSagaManager.RUN_FAILED);
    assertThat(transaction.states.get("orders:order-42").getOutcome())
        .isEqualTo(TransactionalSagaManager.OUTCOME_FAILED);
  }

  @Test
  void only_explicit_new_run_restarts_sequence() throws Exception {
    MemoryTransaction transaction = new MemoryTransaction();
    TransactionalSagaManager manager =
        manager(
            transaction,
            Map.of(
                "OrderCreated",
                (state, ignored) -> {
                  state.setStatus(SagaState.COMPLETED);
                  return List.of();
                }));

    manager.handle(event("event-1", "OrderCreated"));
    String firstRun = transaction.states.get("orders:order-42").getRunId();
    SagaState next = manager.startNewRun("order-42");

    assertThat(next.getRunId()).isNotEqualTo(firstRun);
    assertThat(transaction.history.get(transaction.history.size() - 1).getSequence()).isEqualTo(1L);
    assertThat(transaction.history.get(transaction.history.size() - 1).getEventType())
        .isEqualTo(TransactionalSagaManager.RUN_STARTED);
  }

  @Test
  void preserves_handler_failure_and_records_it_after_the_previous_run() throws Exception {
    MemoryTransaction transaction = new MemoryTransaction();
    Map<String, SagaDefinition.Handler> handlers = new HashMap<>();
    handlers.put(
        "OrderCreated",
        (state, ignored) -> {
          state.setStatus(SagaState.WAITING);
          state.setStepId("reserve");
          return List.of();
        });
    handlers.put(
        "OrderRetry",
        (state, ignored) -> {
          throw new IllegalStateException("unavailable");
        });
    TransactionalSagaManager manager = manager(transaction, handlers);
    manager.handle(event("event-1", "OrderCreated"));
    int before = transaction.history.size();

    assertThatThrownBy(() -> manager.handle(event("event-2", "OrderRetry")))
        .hasMessage("unavailable");
    assertThat(transaction.history).hasSize(before + 1);
    assertThat(transaction.history.get(before).getEventType())
        .isEqualTo(TransactionalSagaManager.STEP_FAILED);
    assertThat(transaction.failed)
        .singleElement()
        .satisfies(value -> assertThat(value).contains("event-2:unavailable"));
  }
}
