package io.cursus.client.framework;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.cursus.client.framework.Saga.Command;
import io.cursus.client.framework.Saga.Definition;
import io.cursus.client.framework.Saga.Manager;
import io.cursus.client.framework.Saga.State;
import io.cursus.client.framework.Saga.Transaction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class SagaTest {
  private static final class MemoryRepository implements Saga.Repository {
    private final Set<String> claimed = new HashSet<>();
    private final Map<String, State> states = new HashMap<>();
    private final List<Command> commands = new ArrayList<>();
    private final List<String> completed = new ArrayList<>();

    @Override
    public void transact(Consumer<Transaction> apply) {
      Set<String> oldClaimed = new HashSet<>(claimed);
      Map<String, State> oldStates = new HashMap<>(states);
      List<Command> oldCommands = new ArrayList<>(commands);
      List<String> oldCompleted = new ArrayList<>(completed);
      try {
        apply.accept(
            new Transaction() {
              @Override
              public boolean claim(String consumer, String eventId) {
                return claimed.add(consumer + ":" + eventId);
              }

              @Override
              public State load(String type, String association) {
                State state = states.get(type + ":" + association);
                return copy(state);
              }

              @Override
              public void saveCas(State state, long expected) {
                State current = states.get(state.getType() + ":" + state.getId());
                if ((current == null ? 0 : current.getVersion()) != expected) {
                  throw new IllegalStateException("saga version conflict");
                }
                states.put(state.getType() + ":" + state.getId(), copy(state));
              }

              @Override
              public void enqueue(Command command) {
                if (commands.stream().noneMatch(value -> value.getId().equals(command.getId()))) {
                  commands.add(command);
                }
              }

              @Override
              public void complete(String consumer, String eventId) {
                completed.add(eventId);
              }

              @Override
              public void fail(String consumer, String eventId, RuntimeException cause) {}
            });
      } catch (RuntimeException exception) {
        claimed.clear();
        claimed.addAll(oldClaimed);
        states.clear();
        states.putAll(oldStates);
        commands.clear();
        commands.addAll(oldCommands);
        completed.clear();
        completed.addAll(oldCompleted);
        throw exception;
      }
    }

    private static State copy(State source) {
      if (source == null) return null;
      Map<String, Saga.EffectState> effects = new HashMap<>();
      source
          .getEffects()
          .forEach(
              (key, value) ->
                  effects.put(
                      key,
                      new Saga.EffectState(
                          value.getId(),
                          value.getStep(),
                          value.getStatus(),
                          value.getCommandId(),
                          value.getAttempts(),
                          value.getLastError(),
                          value.getUpdatedAt())));
      Saga.CompensationState compensation = null;
      if (source.getCompensation() != null) {
        Saga.CompensationState value = source.getCompensation();
        compensation =
            new Saga.CompensationState(
                value.getStep(),
                value.getStatus(),
                value.getAttempts(),
                value.getLastError(),
                value.getUpdatedAt());
      }
      return new State(
          source.getId(),
          source.getType(),
          source.getAssociationKey(),
          source.getCorrelationId(),
          source.getStatus(),
          source.getStep(),
          source.getData(),
          source.getRetryCount(),
          source.getLastError(),
          source.getUpdatedAt(),
          source.getVersion(),
          effects,
          compensation);
    }
  }

  private static EventEnvelope event() {
    EventEnvelope source =
        EventEnvelope.create("game", "game-1", "GameFinished", Map.of("winner", "p1"));
    return new EventEnvelope(
        "event-1",
        source.eventType(),
        1,
        source.aggregateType(),
        source.aggregateId(),
        1,
        source.occurredAt(),
        "saga-1",
        "",
        "",
        source.payload());
  }

  @Test
  void claimStateAndOutboxAreAtomicAndIdempotent() {
    MemoryRepository repository = new MemoryRepository();
    Manager manager =
        new Manager(
            new Definition(
                "finish-game",
                Map.of(
                    "GameFinished",
                    (state, event) -> {
                      state.setStatus(Saga.WAITING);
                      return List.of(Command.builder().type("UpdatePlayerElo").build());
                    })),
            repository);

    manager.handle(event());
    manager.handle(event());

    assertThat(repository.commands).hasSize(1);
    assertThat(repository.commands.get(0).getId()).isEqualTo("finish-game:saga-1:event-1:0");
    State state = repository.states.get("finish-game:saga-1");
    assertThat(state.getStatus()).isEqualTo(Saga.WAITING);
    assertThat(state.getEffects().get("event-1:0").getStatus()).isEqualTo(Saga.EFFECT_ENQUEUED);
  }

  @Test
  void effectCommandFenceAndCompensationLifecycleMatchGoContract() {
    MemoryRepository repository = new MemoryRepository();
    Manager manager =
        new Manager(
            new Definition(
                "finish-game",
                Map.of(
                    "GameFinished",
                    (state, event) ->
                        List.of(Command.builder().effectId("elo").type("UpdateElo").build()))),
            repository);
    manager.handle(event());
    String commandId =
        repository.states.get("finish-game:saga-1").getEffects().get("elo").getCommandId();

    assertThatThrownBy(() -> manager.acknowledgeEffect("saga-1", "elo", "stale"))
        .hasMessageContaining("fence");
    manager.acknowledgeEffect("saga-1", "elo", commandId);
    assertThat(repository.states.get("finish-game:saga-1").getEffects().get("elo").getStatus())
        .isEqualTo(Saga.EFFECT_SUCCEEDED);
    assertThat(
            manager
                .startCompensation("saga-1", "rollback", new RuntimeException("failed"))
                .getStatus())
        .isEqualTo(Saga.COMPENSATING);
    manager.completeCompensation("saga-1");
    assertThat(repository.states.get("finish-game:saga-1").getStatus()).isEqualTo(Saga.COMPLETED);
  }
}
