package io.cursus.client.framework;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Durable saga inbox/state/outbox contract equivalent to the Go SDK framework. */
public final class Saga {
  public static final String RUNNING = "RUNNING";
  public static final String WAITING = "WAITING";
  public static final String COMPLETED = "COMPLETED";
  public static final String COMPENSATING = "COMPENSATING";
  public static final String FAILED = "FAILED";
  public static final String EFFECT_ENQUEUED = "ENQUEUED";
  public static final String EFFECT_SUCCEEDED = "SUCCEEDED";
  public static final String EFFECT_FAILED = "FAILED";

  private Saga() {}

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class EffectState {
    private String id;
    private String step;
    private String status;
    private String commandId;
    private int attempts;
    private String lastError;
    private Instant updatedAt;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CompensationState {
    private String step;
    private String status;
    private int attempts;
    private String lastError;
    private Instant updatedAt;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class State {
    private String id;
    private String type;
    private String associationKey;
    private String correlationId;
    @Builder.Default private String status = RUNNING;
    private String step;
    private String data;
    private int retryCount;
    private String lastError;
    private Instant updatedAt;
    private long version;
    @Builder.Default private Map<String, EffectState> effects = new LinkedHashMap<>();
    private CompensationState compensation;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Command {
    private String id;
    private String effectId;
    private String type;
    private String sagaId;
    private String correlationId;
    private String causationId;
    private String payload;
  }

  public interface Transaction {
    boolean claim(String consumer, String eventId);

    State load(String sagaType, String associationKey);

    void saveCas(State state, long expectedVersion);

    void enqueue(Command command);

    void complete(String consumer, String eventId);

    void fail(String consumer, String eventId, RuntimeException cause);
  }

  public interface Repository {
    /** Implementations must roll back every operation if the callback throws. */
    void transact(Consumer<Transaction> apply);
  }

  @FunctionalInterface
  public interface Handler {
    List<Command> handle(State state, EventEnvelope event);
  }

  public record Definition(String type, Map<String, Handler> handlers) {
    public Definition {
      handlers = handlers == null ? Map.of() : Map.copyOf(handlers);
    }
  }

  public static final class Manager {
    private final Definition definition;
    private final Repository repository;

    public Manager(Definition definition, Repository repository) {
      if (definition == null || blank(definition.type()) || definition.handlers().isEmpty()) {
        throw new IllegalArgumentException("saga definition requires a type and handlers");
      }
      if (repository == null) throw new IllegalArgumentException("saga repository is required");
      this.definition = definition;
      this.repository = repository;
    }

    public void handle(EventEnvelope event) {
      if (blank(event.eventId()) || blank(event.eventType())) {
        throw new IllegalArgumentException("saga event identity is incomplete");
      }
      String association =
          firstNonBlank(event.associationKey(), event.correlationId(), event.aggregateId());
      if (blank(association))
        throw new IllegalArgumentException("saga association key is required");
      RuntimeException[] handlerFailure = new RuntimeException[1];
      repository.transact(
          transaction -> {
            if (!transaction.claim(definition.type(), event.eventId())) return;
            Loaded loaded = loadOrCreate(transaction, association);
            State state = loaded.state();
            if (blank(state.getCorrelationId())) state.setCorrelationId(event.correlationId());
            Handler handler = definition.handlers().get(event.eventType());
            if (handler == null) {
              transaction.complete(definition.type(), event.eventId());
              return;
            }
            List<Command> commands;
            try {
              commands = handler.handle(state, event);
            } catch (RuntimeException exception) {
              state.setRetryCount(state.getRetryCount() + 1);
              state.setLastError(exception.getMessage());
              state.setUpdatedAt(Instant.now());
              save(transaction, state, loaded.version());
              transaction.fail(definition.type(), event.eventId(), exception);
              handlerFailure[0] = exception;
              return;
            }
            state.setLastError("");
            for (int index = 0; index < commands.size(); index++) {
              Command command = commands.get(index);
              if (blank(command.getType())) {
                throw new IllegalArgumentException(
                    "saga command type is required at index " + index);
              }
              String effectId =
                  blank(command.getEffectId())
                      ? event.eventId() + ":" + index
                      : command.getEffectId();
              EffectState existing = state.getEffects().get(effectId);
              if (existing != null
                  && (EFFECT_ENQUEUED.equals(existing.getStatus())
                      || EFFECT_SUCCEEDED.equals(existing.getStatus()))) continue;
              command.setEffectId(effectId);
              if (blank(command.getSagaId())) command.setSagaId(state.getId());
              if (blank(command.getCorrelationId())) {
                command.setCorrelationId(state.getCorrelationId());
              }
              if (blank(command.getCausationId())) command.setCausationId(event.eventId());
              command.setId(definition.type() + ":" + state.getId() + ":" + effectId);
              transaction.enqueue(command);
              EffectState effect = existing == null ? new EffectState() : existing;
              effect.setId(effectId);
              effect.setStep(command.getType());
              effect.setStatus(EFFECT_ENQUEUED);
              effect.setCommandId(command.getId());
              effect.setAttempts(effect.getAttempts() + 1);
              effect.setLastError("");
              effect.setUpdatedAt(Instant.now());
              state.getEffects().put(effectId, effect);
            }
            state.setUpdatedAt(Instant.now());
            save(transaction, state, loaded.version());
            transaction.complete(definition.type(), event.eventId());
          });
      if (handlerFailure[0] != null) throw handlerFailure[0];
    }

    public void acknowledgeEffect(String association, String effectId, String commandId) {
      updateEffect(association, effectId, commandId, EFFECT_SUCCEEDED, null);
    }

    public void failEffect(
        String association, String effectId, String commandId, RuntimeException cause) {
      if (cause == null) throw new IllegalArgumentException("effect failure is required");
      updateEffect(association, effectId, commandId, EFFECT_FAILED, cause);
    }

    private void updateEffect(
        String association,
        String effectId,
        String commandId,
        String status,
        RuntimeException cause) {
      if (blank(effectId) || blank(commandId)) {
        throw new IllegalArgumentException("effect and command identities are required");
      }
      repository.transact(
          transaction -> {
            Loaded loaded = loadOrCreate(transaction, association);
            EffectState effect = loaded.state().getEffects().get(effectId);
            if (effect == null) {
              throw new IllegalArgumentException("effect does not exist: " + effectId);
            }
            if (!commandId.equals(effect.getCommandId())) {
              throw new IllegalArgumentException("effect command fence mismatch: " + effectId);
            }
            if (status.equals(effect.getStatus())) return;
            if (!EFFECT_ENQUEUED.equals(effect.getStatus())) {
              throw new IllegalArgumentException(
                  "effect is not awaiting acknowledgement: " + effectId);
            }
            effect.setStatus(status);
            effect.setLastError(cause == null ? "" : cause.getMessage());
            effect.setUpdatedAt(Instant.now());
            loaded.state().setUpdatedAt(Instant.now());
            save(transaction, loaded.state(), loaded.version());
          });
    }

    public State startCompensation(String association, String step, RuntimeException cause) {
      if (blank(step)) throw new IllegalArgumentException("compensation step is required");
      State[] result = new State[1];
      repository.transact(
          transaction -> {
            Loaded loaded = loadOrCreate(transaction, association);
            CompensationState compensation = loaded.state().getCompensation();
            if (compensation == null) compensation = new CompensationState();
            compensation.setStep(step);
            compensation.setStatus(COMPENSATING);
            compensation.setAttempts(compensation.getAttempts() + 1);
            compensation.setLastError(cause == null ? "" : cause.getMessage());
            compensation.setUpdatedAt(Instant.now());
            loaded.state().setCompensation(compensation);
            loaded.state().setStatus(COMPENSATING);
            loaded.state().setUpdatedAt(Instant.now());
            save(transaction, loaded.state(), loaded.version());
            result[0] = loaded.state();
          });
      return result[0];
    }

    public void completeCompensation(String association) {
      updateCompensation(association, COMPLETED, null);
    }

    public void failCompensation(String association, RuntimeException cause) {
      if (cause == null) throw new IllegalArgumentException("compensation failure is required");
      updateCompensation(association, FAILED, cause);
      throw cause;
    }

    private void updateCompensation(String association, String status, RuntimeException cause) {
      repository.transact(
          transaction -> {
            Loaded loaded = loadOrCreate(transaction, association);
            CompensationState compensation = loaded.state().getCompensation();
            if (compensation == null || blank(compensation.getStep())) {
              throw new IllegalArgumentException("compensation is not active");
            }
            compensation.setStatus(status);
            compensation.setLastError(cause == null ? "" : cause.getMessage());
            compensation.setUpdatedAt(Instant.now());
            loaded.state().setStatus(status);
            loaded.state().setUpdatedAt(Instant.now());
            save(transaction, loaded.state(), loaded.version());
          });
    }

    private Loaded loadOrCreate(Transaction transaction, String association) {
      if (blank(association)) throw new IllegalArgumentException("association key is required");
      State state = transaction.load(definition.type(), association);
      if (state == null) {
        state =
            State.builder()
                .id(association)
                .type(definition.type())
                .associationKey(association)
                .status(RUNNING)
                .build();
      }
      if (state.getEffects() == null) state.setEffects(new LinkedHashMap<>());
      return new Loaded(state, state.getVersion());
    }

    private static void save(Transaction transaction, State state, long expected) {
      state.setVersion(expected + 1);
      try {
        transaction.saveCas(state, expected);
      } catch (RuntimeException exception) {
        state.setVersion(expected);
        throw exception;
      }
    }
  }

  public static Command compensationCommand(
      String commandType, State state, String causationId, String payload) {
    return Command.builder()
        .type(commandType)
        .sagaId(state.getId())
        .correlationId(state.getCorrelationId())
        .causationId(causationId)
        .payload(payload)
        .build();
  }

  private record Loaded(State state, long version) {}

  private static String firstNonBlank(String... values) {
    for (String value : values) if (!blank(value)) return value;
    return "";
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
