package io.cursus.client.producer;

import io.cursus.client.message.CursusMessage;
import java.util.List;
import lombok.Data;

/** Tracks the state of a sent batch for acknowledgment and retry. */
@Data
public class BatchState {
  private final long batchId;
  private final List<CursusMessage> messages;
  private final long sentAtMs;
  private volatile boolean acknowledged;

  public BatchState(long batchId, List<CursusMessage> messages) {
    this.batchId = batchId;
    this.messages = messages;
    this.sentAtMs = System.currentTimeMillis();
  }
}
