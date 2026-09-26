package io.cursus.client.saga;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Durable, service-owned state for one Saga instance. */
public final class SagaState {
  public static final String RUNNING = "RUNNING";
  public static final String WAITING = "WAITING";
  public static final String COMPLETED = "COMPLETED";
  public static final String COMPENSATING = "COMPENSATING";
  public static final String FAILED = "FAILED";
  public static final String PENDING = "PENDING";
  public static final String EFFECT_SUCCEEDED = "SUCCEEDED";
  public static final String EFFECT_FAILED = "FAILED";

  private final String sagaId;
  private final String sagaType;
  private final String associationKey;
  private String correlationId = "";
  private String status = RUNNING;
  private String stepId = "";
  private Map<String, Object> data = new LinkedHashMap<>();
  private int retryCount;
  private String lastError = "";
  private String runId = UUID.randomUUID().toString();
  private long nextSequence;
  private String outcome = "";
  private Instant updatedAt = Instant.now();
  private Map<String, EffectState> effects = new LinkedHashMap<>();
  private CompensationState compensation;

  public SagaState(String sagaId, String sagaType, String associationKey) {
    this.sagaId = sagaId;
    this.sagaType = sagaType;
    this.associationKey = associationKey;
  }

  public String getSagaId() {
    return sagaId;
  }

  public String getSagaType() {
    return sagaType;
  }

  public String getAssociationKey() {
    return associationKey;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  public void setCorrelationId(String value) {
    correlationId = value == null ? "" : value;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String value) {
    status = value;
  }

  public String getStepId() {
    return stepId;
  }

  public void setStepId(String value) {
    stepId = value == null ? "" : value;
  }

  public Map<String, Object> getData() {
    return data;
  }

  public void setData(Map<String, Object> value) {
    data = value;
  }

  public int getRetryCount() {
    return retryCount;
  }

  public void setRetryCount(int value) {
    retryCount = value;
  }

  public String getLastError() {
    return lastError;
  }

  public void setLastError(String value) {
    lastError = value == null ? "" : value;
  }

  public String getRunId() {
    return runId;
  }

  public void setRunId(String value) {
    runId = value;
  }

  public long getNextSequence() {
    return nextSequence;
  }

  public void setNextSequence(long value) {
    nextSequence = value;
  }

  public long nextSequence() {
    return ++nextSequence;
  }

  public String getOutcome() {
    return outcome;
  }

  public void setOutcome(String value) {
    outcome = value == null ? "" : value;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Instant value) {
    updatedAt = value;
  }

  public Map<String, EffectState> getEffects() {
    return effects;
  }

  public void setEffects(Map<String, EffectState> value) {
    effects = value;
  }

  public CompensationState getCompensation() {
    return compensation;
  }

  public void setCompensation(CompensationState value) {
    compensation = value;
  }

  public static final class EffectState {
    private String effectId;
    private String stepId;
    private String status = PENDING;
    private String commandId = "";
    private boolean published;
    private int attempts;
    private String lastError = "";
    private Instant updatedAt = Instant.now();

    public EffectState(String effectId, String stepId) {
      this.effectId = effectId;
      this.stepId = stepId;
    }

    public String getEffectId() {
      return effectId;
    }

    public String getStepId() {
      return stepId;
    }

    public void setStepId(String value) {
      stepId = value;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String value) {
      status = value;
    }

    public String getCommandId() {
      return commandId;
    }

    public void setCommandId(String value) {
      commandId = value;
    }

    public boolean isPublished() {
      return published;
    }

    public void setPublished(boolean value) {
      published = value;
    }

    public int getAttempts() {
      return attempts;
    }

    public void setAttempts(int value) {
      attempts = value;
    }

    public String getLastError() {
      return lastError;
    }

    public void setLastError(String value) {
      lastError = value;
    }

    public Instant getUpdatedAt() {
      return updatedAt;
    }

    public void setUpdatedAt(Instant value) {
      updatedAt = value;
    }
  }

  public static final class CompensationState {
    private String stepId;
    private String status = COMPENSATING;
    private int attempts;
    private String lastError = "";
    private Instant updatedAt = Instant.now();

    public CompensationState(String stepId) {
      this.stepId = stepId;
    }

    public String getStepId() {
      return stepId;
    }

    public void setStepId(String value) {
      stepId = value;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String value) {
      status = value;
    }

    public int getAttempts() {
      return attempts;
    }

    public void setAttempts(int value) {
      attempts = value;
    }

    public String getLastError() {
      return lastError;
    }

    public void setLastError(String value) {
      lastError = value;
    }

    public Instant getUpdatedAt() {
      return updatedAt;
    }

    public void setUpdatedAt(Instant value) {
      updatedAt = value;
    }
  }
}
