package io.cursus.client.exception;

/** Thrown when an operation is attempted on a closed producer. */
public class CursusProducerClosedException extends CursusException {
  public CursusProducerClosedException() {
    super("Producer is closed");
  }
}
