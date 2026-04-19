package io.cursus.client.exception;

/** Thrown when a protocol encoding or decoding error occurs. */
public class CursusProtocolException extends CursusException {
  public CursusProtocolException(String message) {
    super(message);
  }

  public CursusProtocolException(String message, Throwable cause) {
    super(message, cause);
  }
}
