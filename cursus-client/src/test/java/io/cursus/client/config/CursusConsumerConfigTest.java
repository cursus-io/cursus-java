package io.cursus.client.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class CursusConsumerConfigTest {

  @Test
  void builderSetsDefaults() {
    CursusConsumerConfig config =
        CursusConsumerConfig.builder().topic("test-topic").groupId("test-group").build();

    assertThat(config.getConsumerMode()).isEqualTo(ConsumerMode.STREAMING);
    assertThat(config.getAutoCommitInterval()).isEqualTo(Duration.ofSeconds(5));
    assertThat(config.getMaxPollRecords()).isEqualTo(100);
    assertThat(config.getSessionTimeoutMs()).isEqualTo(30000);
    assertThat(config.getHeartbeatIntervalMs()).isEqualTo(3000);
  }

  @Test
  void builderOverridesDefaults() {
    CursusConsumerConfig config =
        CursusConsumerConfig.builder()
            .topic("events")
            .groupId("workers")
            .consumerMode(ConsumerMode.POLLING)
            .autoCommitInterval(Duration.ofSeconds(10))
            .maxPollRecords(500)
            .build();

    assertThat(config.getConsumerMode()).isEqualTo(ConsumerMode.POLLING);
    assertThat(config.getAutoCommitInterval()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.getMaxPollRecords()).isEqualTo(500);
  }
}
