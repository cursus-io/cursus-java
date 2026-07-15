package io.cursus.client.exception;

import java.util.Map;

public class CursusBrokerException extends CursusException {
  private final String code;
  private final Map<String, String> fields;

  public CursusBrokerException(String code, Map<String, String> fields, String response) {
    super(response);
    this.code = code;
    this.fields = fields;
  }

  public String getCode() {
    return code;
  }

  public Map<String, String> getFields() {
    return fields;
  }
}
