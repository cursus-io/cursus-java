package io.cursus.client.eventstore;

import lombok.Getter;

@Getter
public class StreamEvent {
  private final long version;
  private final long offset;
  private final String type;
  private final int schemaVersion;
  private final String payload;
  private final String metadata;

  public StreamEvent(long version, long offset, String type, int schemaVersion, String payload, String metadata) {
    this.version = version;
    this.offset = offset;
    this.type = type;
    this.schemaVersion = schemaVersion;
    this.payload = payload;
    this.metadata = metadata;
  }
}
