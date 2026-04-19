package io.cursus.client.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class CursusProducerConfigTest {

  @Test
  void builderSetsDefaults() {
    CursusProducerConfig config = CursusProducerConfig.builder().topic("test-topic").build();

    assertThat(config.getBrokers()).containsExactly("localhost:9000");
    assertThat(config.getTopic()).isEqualTo("test-topic");
    assertThat(config.getPartitions()).isEqualTo(4);
    assertThat(config.getAcks()).isEqualTo(Acks.ONE);
    assertThat(config.getBatchSize()).isEqualTo(500);
    assertThat(config.getLingerMs()).isEqualTo(100);
    assertThat(config.isIdempotent()).isFalse();
    assertThat(config.getCompressionType()).isEqualTo("none");
    assertThat(config.getMaxInflightRequests()).isEqualTo(5);
    assertThat(config.getFlushTimeoutMs()).isEqualTo(30000);
  }

  @Test
  void builderOverridesDefaults() {
    CursusProducerConfig config =
        CursusProducerConfig.builder()
            .brokers(List.of("broker1:9000", "broker2:9000"))
            .topic("my-topic")
            .acks(Acks.ALL)
            .batchSize(1000)
            .idempotent(true)
            .compressionType("gzip")
            .build();

    assertThat(config.getBrokers()).hasSize(2);
    assertThat(config.getAcks()).isEqualTo(Acks.ALL);
    assertThat(config.getBatchSize()).isEqualTo(1000);
    assertThat(config.isIdempotent()).isTrue();
    assertThat(config.getCompressionType()).isEqualTo("gzip");
  }
}
