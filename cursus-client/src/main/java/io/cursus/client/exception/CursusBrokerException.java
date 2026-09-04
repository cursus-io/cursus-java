package io.cursus.client.exception;

import java.util.Map;

public class CursusBrokerException extends CursusException {
  private final String code;
  private final Map<String, String> fields;
  private final String errorClass;
  private final boolean retryable;

  public CursusBrokerException(String code, Map<String, String> fields, String response) {
    super(response);
    this.code = code;
    this.fields = Map.copyOf(fields);
    this.errorClass = fields.getOrDefault("class", "");
    this.retryable = Boolean.parseBoolean(fields.getOrDefault("retryable", "false"));
  }

  public String getCode() {
    return code;
  }

  public Map<String, String> getFields() {
    return fields;
  }

  public String getErrorClass() {
    return errorClass;
  }

  public boolean isRetryable() {
    return retryable;
  }

  public boolean canRetry(boolean idempotent) {
    return retryable && idempotent;
  }
}
