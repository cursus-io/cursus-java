package io.cursus.client.exception;

import java.util.Map;

public class CursusAuthenticationException extends CursusBrokerException {
  public CursusAuthenticationException(String code, Map<String, String> fields, String response) {
    super(code, fields, response);
  }
}
