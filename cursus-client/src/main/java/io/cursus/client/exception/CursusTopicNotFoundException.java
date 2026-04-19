package io.cursus.client.exception;

/** Thrown when the requested topic does not exist on the broker. */
public class CursusTopicNotFoundException extends CursusException {
  public CursusTopicNotFoundException(String topic) {
    super("Topic not found: " + topic);
  }
}
