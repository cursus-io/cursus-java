package io.cursus.client.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import io.cursus.client.config.CursusConsumerConfig;
import org.junit.jupiter.api.Test;

class CursusConsumerLifecycleTest {

  @Test
  void requestRebalanceSetsTopLevelSignal() {
    CursusConsumer consumer =
        new CursusConsumer(
            CursusConsumerConfig.builder().topic("orders").groupId("workers").build());

    consumer.requestRebalance();

    assertThat(consumer.isRebalanceRequired()).isTrue();
  }
}
