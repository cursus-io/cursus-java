package io.cursus.client.exception;

import java.util.Map;

public class CursusAuthorizationException extends CursusBrokerException {
  public CursusAuthorizationException(String code, Map<String, String> fields, String response) {
    super(code, fields, response);
  }
}
