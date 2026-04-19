package io.cursus.client.producer;

import static org.assertj.core.api.Assertions.assertThat;

import io.cursus.client.message.CursusMessage;
import java.util.List;
import org.junit.jupiter.api.Test;

class PartitionBufferTest {

  @Test
  void bufferAccumulatesMessages() {
    PartitionBuffer buffer = new PartitionBuffer(0, 3, 1000);
    buffer.add("msg-1", null);
    buffer.add("msg-2", null);
    assertThat(buffer.pendingCount()).isEqualTo(2);
  }

  @Test
  void drainReturnsBatchWhenFull() {
    PartitionBuffer buffer = new PartitionBuffer(0, 2, 1000);
    buffer.add("msg-1", null);
    buffer.add("msg-2", null);
    List<CursusMessage> batch = buffer.drain();
    assertThat(batch).hasSize(2);
    assertThat(batch.get(0).getPayload()).isEqualTo("msg-1");
    assertThat(batch.get(1).getPayload()).isEqualTo("msg-2");
    assertThat(buffer.pendingCount()).isZero();
  }

  @Test
  void drainReturnsEmptyWhenNotFull() {
    PartitionBuffer buffer = new PartitionBuffer(0, 5, 1000);
    buffer.add("msg-1", null);
    List<CursusMessage> batch = buffer.drain();
    assertThat(batch).isEmpty();
  }

  @Test
  void forceFlushDrainsAll() {
    PartitionBuffer buffer = new PartitionBuffer(0, 100, 1000);
    buffer.add("a", null);
    buffer.add("b", null);
    List<CursusMessage> batch = buffer.forceFlush();
    assertThat(batch).hasSize(2);
  }

  @Test
  void sequenceNumberIncrements() {
    PartitionBuffer buffer = new PartitionBuffer(0, 10, 1000);
    buffer.add("a", null);
    buffer.add("b", null);
    List<CursusMessage> batch = buffer.forceFlush();
    assertThat(batch.get(0).getSeqNum()).isEqualTo(1);
    assertThat(batch.get(1).getSeqNum()).isEqualTo(2);
  }
}
