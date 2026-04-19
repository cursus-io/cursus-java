package io.cursus.client.exception;

/** Base exception for all Cursus client errors. */
public class CursusException extends RuntimeException {

  public CursusException(String message) {
    super(message);
  }

  public CursusException(String message, Throwable cause) {
    super(message, cause);
  }
}
