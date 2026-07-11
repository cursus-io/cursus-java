package io.cursus.client.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class CursusProducerRoutingTest {

  @Test
  void roundRobinRoutingUsesVerifiedPartitionCount() {
    assertThat(CursusProducer.selectPartition(null, 0, 2)).isEqualTo(0);
    assertThat(CursusProducer.selectPartition(null, 1, 2)).isEqualTo(1);
    assertThat(CursusProducer.selectPartition(null, 2, 2)).isEqualTo(0);
    assertThat(CursusProducer.selectPartition(null, -1, 2)).isEqualTo(1);
  }

  @Test
  void keyRoutingUsesVerifiedPartitionCount() {
    int partition = CursusProducer.selectPartition("order-123", 0, 2);

    assertThat(partition).isBetween(0, 1);
  }

  @Test
  void routingRejectsInvalidPartitionCount() {
    assertThatThrownBy(() -> CursusProducer.selectPartition(null, 0, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partitionCount");
  }
}
