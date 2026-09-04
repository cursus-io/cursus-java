package io.cursus.client.framework;

public interface SnapshotRestorer {
  void restoreSnapshot(String payload, long version);
}
