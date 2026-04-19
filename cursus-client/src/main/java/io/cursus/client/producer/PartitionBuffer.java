package io.cursus.client.producer;

import io.cursus.client.message.CursusMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/** Per-partition message buffer that accumulates messages into batches. Thread-safe. */
public class PartitionBuffer {

  private final int partitionId;
  private final int batchSize;
  private final int bufferCapacity;
  private final AtomicLong seqNumGenerator = new AtomicLong(0);
  private final ReentrantLock lock = new ReentrantLock();
  private final List<CursusMessage> buffer;

  public PartitionBuffer(int partitionId, int batchSize, int bufferCapacity) {
    this.partitionId = partitionId;
    this.batchSize = batchSize;
    this.bufferCapacity = bufferCapacity;
    this.buffer = new ArrayList<>(batchSize);
  }

  public long add(String payload, String key) {
    long seq = seqNumGenerator.incrementAndGet();
    CursusMessage msg =
        CursusMessage.builder()
            .seqNum(seq)
            .payload(payload)
            .key(key)
            .offset(0)
            .epoch(0)
            .eventType("")
            .schemaVersion(0)
            .aggregateVersion(0)
            .metadata("")
            .build();
    lock.lock();
    try {
      buffer.add(msg);
    } finally {
      lock.unlock();
    }
    return seq;
  }

  public List<CursusMessage> drain() {
    lock.lock();
    try {
      if (buffer.size() >= batchSize) {
        List<CursusMessage> batch = new ArrayList<>(buffer);
        buffer.clear();
        return batch;
      }
      return Collections.emptyList();
    } finally {
      lock.unlock();
    }
  }

  public List<CursusMessage> forceFlush() {
    lock.lock();
    try {
      if (buffer.isEmpty()) return Collections.emptyList();
      List<CursusMessage> batch = new ArrayList<>(buffer);
      buffer.clear();
      return batch;
    } finally {
      lock.unlock();
    }
  }

  public int pendingCount() {
    lock.lock();
    try {
      return buffer.size();
    } finally {
      lock.unlock();
    }
  }

  public int getPartitionId() {
    return partitionId;
  }
}
