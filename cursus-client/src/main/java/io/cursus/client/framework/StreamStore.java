package io.cursus.client.framework;

import io.cursus.client.eventstore.AppendResult;
import io.cursus.client.eventstore.Event;
import io.cursus.client.eventstore.StreamData;

public interface StreamStore {
  StreamData readStream(String key);

  AppendResult append(String key, long expectedVersion, Event event);
}
