package io.cursus.client.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FnvHashTest {

  @Test
  void sameKeyProducesSameHash() {
    int hash1 = FnvHash.fnv1a("order-123");
    int hash2 = FnvHash.fnv1a("order-123");
    assertThat(hash1).isEqualTo(hash2);
  }

  @Test
  void differentKeysProduceDifferentHashes() {
    int hash1 = FnvHash.fnv1a("key-a");
    int hash2 = FnvHash.fnv1a("key-b");
    assertThat(hash1).isNotEqualTo(hash2);
  }

  @Test
  void partitionAssignmentIsConsistent() {
    int partition1 = FnvHash.partition("order-123", 4);
    int partition2 = FnvHash.partition("order-123", 4);
    assertThat(partition1).isEqualTo(partition2);
    assertThat(partition1).isBetween(0, 3);
  }

  @Test
  void partitionDistributesAcrossRange() {
    boolean[] seen = new boolean[4];
    for (int i = 0; i < 1000; i++) {
      int p = FnvHash.partition("key-" + i, 4);
      seen[p] = true;
    }
    for (boolean s : seen) {
      assertThat(s).isTrue();
    }
  }
}
