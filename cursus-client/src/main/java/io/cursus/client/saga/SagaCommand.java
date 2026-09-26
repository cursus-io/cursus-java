package io.cursus.client.saga;

import java.util.UUID;

/** A command inserted into the service-owned command outbox. */
public final class SagaCommand {
  private final String type;
  private final String payload;
  private String effectId = "";
  private String commandId = "";
  private String sagaType = "";
  private String sagaId = "";
  private String correlationId = "";
  private String causationId = "";

  public SagaCommand(String type, String payload) {
    this.type = type;
    this.payload = payload == null ? "" : payload;
  }

  public String getType() {
    return type;
  }

  public String getPayload() {
    return payload;
  }

  public String getEffectId() {
    return effectId;
  }

  public String getCommandId() {
    return commandId;
  }

  public String getSagaType() {
    return sagaType;
  }

  public String getSagaId() {
    return sagaId;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  public String getCausationId() {
    return causationId;
  }

  public SagaCommand effectId(String value) {
    effectId = value;
    return this;
  }

  public SagaCommand commandId(String value) {
    commandId = value;
    return this;
  }

  public SagaCommand sagaType(String value) {
    sagaType = value;
    return this;
  }

  public SagaCommand sagaId(String value) {
    sagaId = value;
    return this;
  }

  public SagaCommand correlationId(String value) {
    correlationId = value;
    return this;
  }

  public SagaCommand causationId(String value) {
    causationId = value;
    return this;
  }

  void ensureCommandId() {
    if (commandId.isEmpty()) commandId = UUID.randomUUID().toString();
  }
}
