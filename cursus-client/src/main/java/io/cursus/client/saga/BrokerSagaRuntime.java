package io.cursus.client.saga;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cursus.client.eventstore.CursusEventStore;
import io.cursus.client.eventstore.StreamData;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Stateful broker-native Saga coordinator.
 *
 * <p>The state stream itself is the inbox and source of truth. Its latest record contains every
 * processed input ID and the complete Saga state, so duplicate delivery commits only its source
 * offset and never creates another run. Use an explicit run ID for every new execution.
 */
public final class BrokerSagaRuntime {
  private static final ObjectMapper JSON = new ObjectMapper();

  public record HistoryDraft(
      String eventType,
      String stepId,
      Integer attempt,
      SagaCommand command,
      String payload,
      String error) {
    public HistoryDraft(String eventType) {
      this(eventType, "", null, null, "", "");
    }
  }

  public record TransitionResult(List<SagaCommand> commands, List<HistoryDraft> history) {
    public TransitionResult {
      commands = commands == null ? List.of() : List.copyOf(commands);
      history = history == null ? List.of() : List.copyOf(history);
    }
  }

  @FunctionalInterface
  public interface Handler {
    TransitionResult handle(SagaState state, SagaEventEnvelope event) throws Exception;
  }

  private final BrokerSagaTransaction transaction;
  private final BrokerSagaTransaction.Config config;
  private final CursusEventStore stateStore;

  public BrokerSagaRuntime(
      BrokerSagaTransaction transaction,
      BrokerSagaTransaction.Config config,
      CursusEventStore stateStore) {
    if (transaction == null || config == null || stateStore == null) {
      throw new IllegalArgumentException("transaction, config, and stateStore are required");
    }
    this.transaction = transaction;
    this.config = config;
    this.stateStore = stateStore;
  }

  public void handle(BrokerSagaTransaction.Input input, Handler handler) throws Exception {
    if (handler == null) throw new IllegalArgumentException("broker Saga handler is required");
    StateRecord record = load(input.sagaId(), input.runId());
    if (record != null && record.processedEventIds.contains(input.event().eventId())) {
      transaction.acknowledgeDuplicate(input);
      return;
    }

    boolean newRun = record == null;
    long currentVersion = newRun ? 0 : record.version;
    if (newRun) {
      SagaState state = new SagaState(input.sagaId(), config.sagaType(), input.sagaId());
      state.setRunId(input.runId());
      state.setCorrelationId(empty(input.event().correlationId()));
      record =
          new StateRecord(
              config.sagaType(), input.sagaId(), input.runId(), state, new ArrayList<>(), 0);
    }

    // A failing handler can mutate the SagaState it receives. Keep those
    // mutations isolated until a successful transition makes them durable.
    SagaState transitionState = copyState(record.state);
    TransitionResult result;
    try {
      result = handler.handle(transitionState, input.event());
    } catch (Exception cause) {
      if (!newRun) recordFailure(input, record, currentVersion, cause);
      throw cause;
    }
    record.state = transitionState;

    List<HistoryDraft> drafts = new ArrayList<>();
    if (newRun) drafts.add(new HistoryDraft("run.started"));
    drafts.addAll(result.history());
    List<SagaCommand> commands = new ArrayList<>();
    for (int index = 0; index < result.commands().size(); index++) {
      SagaCommand command =
          prepareCommand(
              record.state, input.event().eventId(), index, result.commands().get(index));
      commands.add(command);
      drafts.add(
          new HistoryDraft(
              "command.enqueued", command.getType(), null, command, command.getPayload(), ""));
    }

    record.processedEventIds.add(input.event().eventId());
    record.state.setUpdatedAt(Instant.now());
    List<SagaHistoryEvent> history = materialize(input, record.state, drafts);
    transaction.apply(
        input,
        new BrokerSagaTransaction.Transition(
            record.toJson(), currentVersion + 1, commandEnvelopes(record, commands), history));
  }

  public String streamKey(String sagaId, String runId) {
    return transaction.streamKey(sagaId, runId);
  }

  private StateRecord load(String sagaId, String runId) {
    StreamData stream = stateStore.readStream(streamKey(sagaId, runId));
    if (stream.getEvents().isEmpty()) return null;
    StateRecord record =
        StateRecord.fromJson(stream.getEvents().get(stream.getEvents().size() - 1).getPayload());
    if (!config.sagaType().equals(record.sagaType)
        || !sagaId.equals(record.sagaId)
        || !runId.equals(record.runId)) {
      throw new IllegalStateException("broker Saga state stream identity mismatch");
    }
    record.version = stream.getEvents().get(stream.getEvents().size() - 1).getVersion();
    return record;
  }

  private SagaCommand prepareCommand(
      SagaState state, String causationId, int index, SagaCommand command) {
    String effectId = command.getEffectId();
    if (effectId == null || effectId.isBlank()) effectId = causationId + ":" + index;
    String commandId = command.getCommandId();
    if (commandId == null || commandId.isBlank()) {
      commandId =
          BrokerSagaTransaction.deterministicId(
                  "command", config.sagaType(), state.getSagaId(), state.getRunId(), effectId)
              .toString();
    }
    command
        .effectId(effectId)
        .commandId(commandId)
        .sagaType(config.sagaType())
        .sagaId(state.getSagaId())
        .correlationId(
            empty(command.getCorrelationId()).isEmpty()
                ? state.getCorrelationId()
                : command.getCorrelationId())
        .causationId(
            empty(command.getCausationId()).isEmpty() ? causationId : command.getCausationId());
    SagaState.EffectState effect = state.getEffects().get(effectId);
    if (effect == null) effect = new SagaState.EffectState(effectId, command.getType());
    effect.setStepId(command.getType());
    effect.setStatus(SagaState.PENDING);
    effect.setCommandId(commandId);
    effect.setAttempts(effect.getAttempts() + 1);
    effect.setLastError("");
    effect.setUpdatedAt(Instant.now());
    state.getEffects().put(effectId, effect);
    return command;
  }

  private List<BrokerSagaTransaction.CommandEnvelope> commandEnvelopes(
      StateRecord record, List<SagaCommand> commands) {
    return commands.stream()
        .map(
            command ->
                new BrokerSagaTransaction.CommandEnvelope(
                    command.getCommandId(),
                    command.getEffectId(),
                    command.getType(),
                    config.sagaType(),
                    record.sagaId,
                    record.runId,
                    command.getCorrelationId(),
                    command.getCausationId(),
                    command.getPayload()))
        .toList();
  }

  private List<SagaHistoryEvent> materialize(
      BrokerSagaTransaction.Input input, SagaState state, List<HistoryDraft> drafts) {
    List<SagaHistoryEvent> history = new ArrayList<>();
    for (HistoryDraft draft : drafts) {
      history.add(
          transaction.history(
              input,
              state.nextSequence(),
              draft.eventType(),
              empty(draft.stepId()),
              draft.attempt(),
              draft.command(),
              empty(draft.payload()),
              empty(draft.error())));
    }
    return history;
  }

  private void recordFailure(
      BrokerSagaTransaction.Input input, StateRecord record, long currentVersion, Exception cause) {
    record.state.setRetryCount(record.state.getRetryCount() + 1);
    record.state.setLastError(cause.getMessage());
    record.state.setUpdatedAt(Instant.now());
    List<SagaHistoryEvent> history =
        materialize(
            input,
            record.state,
            List.of(
                new HistoryDraft(
                    "step.failed",
                    record.state.getStepId(),
                    record.state.getRetryCount(),
                    null,
                    "",
                    empty(cause.getMessage()))));
    transaction.recordFailure(
        input,
        new BrokerSagaTransaction.Transition(
            record.toJson(), currentVersion + 1, List.of(), history));
  }

  private static final class StateRecord {
    private final String sagaType;
    private final String sagaId;
    private final String runId;
    private SagaState state;
    private final List<String> processedEventIds;
    private long version;

    private StateRecord(
        String sagaType,
        String sagaId,
        String runId,
        SagaState state,
        List<String> processedEventIds,
        long version) {
      this.sagaType = sagaType;
      this.sagaId = sagaId;
      this.runId = runId;
      this.state = state;
      this.processedEventIds = processedEventIds;
      this.version = version;
    }

    private String toJson() {
      try {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("schema_version", 1);
        result.put("saga_type", sagaType);
        result.put("saga_id", sagaId);
        result.put("run_id", runId);
        result.put("state", stateToMap(state));
        result.put("processed_event_ids", processedEventIds);
        result.put("recorded_at", Instant.now().toString());
        return JSON.writeValueAsString(result);
      } catch (Exception exception) {
        throw new IllegalStateException("serialize broker Saga state", exception);
      }
    }

    private static StateRecord fromJson(String raw) {
      try {
        Map<String, Object> value = JSON.readValue(raw, new TypeReference<>() {});
        if (!Integer.valueOf(1).equals(value.get("schema_version"))) {
          throw new IllegalArgumentException("unsupported broker Saga state schema");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> state = (Map<String, Object>) value.get("state");
        @SuppressWarnings("unchecked")
        List<String> processed =
            (List<String>) value.getOrDefault("processed_event_ids", List.of());
        return new StateRecord(
            (String) value.get("saga_type"),
            (String) value.get("saga_id"),
            (String) value.get("run_id"),
            stateFromMap(state),
            new ArrayList<>(processed),
            0);
      } catch (Exception exception) {
        throw new IllegalStateException("decode broker Saga state", exception);
      }
    }
  }

  private static Map<String, Object> stateToMap(SagaState state) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("saga_id", state.getSagaId());
    result.put("saga_type", state.getSagaType());
    result.put("association_key", state.getAssociationKey());
    result.put("correlation_id", state.getCorrelationId());
    result.put("status", state.getStatus());
    result.put("step_id", state.getStepId());
    result.put("data", state.getData());
    result.put("retry_count", state.getRetryCount());
    result.put("last_error", state.getLastError());
    result.put("run_id", state.getRunId());
    result.put("next_sequence", state.getNextSequence());
    result.put("outcome", state.getOutcome());
    result.put("updated_at", state.getUpdatedAt().toString());
    Map<String, Object> effects = new LinkedHashMap<>();
    for (Map.Entry<String, SagaState.EffectState> entry : state.getEffects().entrySet()) {
      SagaState.EffectState effect = entry.getValue();
      effects.put(
          entry.getKey(),
          Map.of(
              "effect_id", effect.getEffectId(),
              "step_id", effect.getStepId(),
              "status", effect.getStatus(),
              "command_id", effect.getCommandId(),
              "published", effect.isPublished(),
              "attempts", effect.getAttempts(),
              "last_error", effect.getLastError(),
              "updated_at", effect.getUpdatedAt().toString()));
    }
    result.put("effects", effects);
    if (state.getCompensation() != null) {
      SagaState.CompensationState value = state.getCompensation();
      result.put(
          "compensation",
          Map.of(
              "step_id", value.getStepId(),
              "status", value.getStatus(),
              "attempts", value.getAttempts(),
              "last_error", value.getLastError(),
              "updated_at", value.getUpdatedAt().toString()));
    }
    return result;
  }

  static SagaState copyState(SagaState state) {
    Map<String, Object> copy =
        JSON.convertValue(JSON.valueToTree(stateToMap(state)), new TypeReference<>() {});
    return stateFromMap(copy);
  }

  @SuppressWarnings("unchecked")
  private static SagaState stateFromMap(Map<String, Object> value) {
    SagaState state =
        new SagaState(
            string(value, "saga_id"), string(value, "saga_type"), string(value, "association_key"));
    state.setCorrelationId(string(value, "correlation_id"));
    state.setStatus(string(value, "status"));
    state.setStepId(string(value, "step_id"));
    state.setData((Map<String, Object>) value.getOrDefault("data", Map.of()));
    state.setRetryCount(number(value, "retry_count").intValue());
    state.setLastError(string(value, "last_error"));
    state.setRunId(string(value, "run_id"));
    state.setNextSequence(number(value, "next_sequence").longValue());
    state.setOutcome(string(value, "outcome"));
    state.setUpdatedAt(Instant.parse(string(value, "updated_at")));
    Map<String, Object> effects = (Map<String, Object>) value.getOrDefault("effects", Map.of());
    for (Map.Entry<String, Object> entry : effects.entrySet()) {
      Map<String, Object> effectMap = (Map<String, Object>) entry.getValue();
      SagaState.EffectState effect =
          new SagaState.EffectState(string(effectMap, "effect_id"), string(effectMap, "step_id"));
      effect.setStatus(string(effectMap, "status"));
      effect.setCommandId(string(effectMap, "command_id"));
      effect.setPublished(Boolean.TRUE.equals(effectMap.get("published")));
      effect.setAttempts(number(effectMap, "attempts").intValue());
      effect.setLastError(string(effectMap, "last_error"));
      effect.setUpdatedAt(Instant.parse(string(effectMap, "updated_at")));
      state.getEffects().put(entry.getKey(), effect);
    }
    Map<String, Object> compensation = (Map<String, Object>) value.get("compensation");
    if (compensation != null) {
      SagaState.CompensationState result =
          new SagaState.CompensationState(string(compensation, "step_id"));
      result.setStatus(string(compensation, "status"));
      result.setAttempts(number(compensation, "attempts").intValue());
      result.setLastError(string(compensation, "last_error"));
      result.setUpdatedAt(Instant.parse(string(compensation, "updated_at")));
      state.setCompensation(result);
    }
    return state;
  }

  private static Number number(Map<String, Object> value, String key) {
    Object result = value.get(key);
    return result instanceof Number number ? number : 0;
  }

  private static String string(Map<String, Object> value, String key) {
    Object result = value.get(key);
    return result == null ? "" : result.toString();
  }

  private static String empty(String value) {
    return value == null ? "" : value;
  }
}
