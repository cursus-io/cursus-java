package io.cursus.client.eventstore;

import java.util.List;
import lombok.Getter;

@Getter
public class StreamData {
  private final Snapshot snapshot;
  private final List<StreamEvent> events;

  public StreamData(Snapshot snapshot, List<StreamEvent> events) {
    this.snapshot = snapshot;
    this.events = events;
  }
}
