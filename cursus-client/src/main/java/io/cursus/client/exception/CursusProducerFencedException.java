package io.cursus.client.exception;

import java.util.Map;

public class CursusProducerFencedException extends CursusBrokerException {
  public CursusProducerFencedException(String code, Map<String, String> fields, String response) {
    super(code, fields, response);
  }
}
