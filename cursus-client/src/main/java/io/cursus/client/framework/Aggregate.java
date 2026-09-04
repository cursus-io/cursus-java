package io.cursus.client.framework;

public interface Aggregate {
  String id();

  String type();

  long version();

  void apply(EventEnvelope event);
}
