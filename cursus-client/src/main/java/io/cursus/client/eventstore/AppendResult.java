package io.cursus.client.eventstore;

import lombok.Getter;

@Getter
public class AppendResult {
  private final long version;
  private final long offset;
  private final int partition;

  public AppendResult(long version, long offset, int partition) {
    this.version = version;
    this.offset = offset;
    this.partition = partition;
  }
}
