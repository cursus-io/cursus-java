package io.cursus.client.eventstore;

import lombok.Getter;

@Getter
public class Snapshot {
  private final long version;
  private final String payload;

  public Snapshot(long version, String payload) {
    this.version = version;
    this.payload = payload;
  }
}
