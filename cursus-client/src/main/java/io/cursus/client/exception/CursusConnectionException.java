package io.cursus.client.exception;

/** Thrown when a connection to the Cursus broker fails or times out. */
public class CursusConnectionException extends CursusException {
  public CursusConnectionException(String message) {
    super(message);
  }

  public CursusConnectionException(String message, Throwable cause) {
    super(message, cause);
  }
}
