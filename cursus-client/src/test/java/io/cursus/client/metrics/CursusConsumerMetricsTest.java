package io.cursus.client.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CursusConsumerMetricsTest {

  private SimpleMeterRegistry registry;
  private CursusConsumerMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new CursusConsumerMetrics(registry, "test-topic", "test-group");
  }

  @Test
  void recordReceivedIncrementsCounter() {
    metrics.recordReceived(10);
    Counter counter = registry.find("cursus.consumer.messages.received").counter();
    assertThat(counter).isNotNull();
    assertThat(counter.count()).isEqualTo(10.0);
  }

  @Test
  void recordProcessedIncrementsCounter() {
    metrics.recordProcessed();
    metrics.recordProcessed();
    Counter counter = registry.find("cursus.consumer.messages.processed").counter();
    assertThat(counter).isNotNull();
    assertThat(counter.count()).isEqualTo(2.0);
  }

  @Test
  void recordProcessLatencyRecordsTimer() {
    metrics.recordProcessLatency(Duration.ofMillis(50));
    Timer timer = registry.find("cursus.consumer.process.latency").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  void recordRebalanceIncrementsCounter() {
    metrics.recordRebalance();
    Counter counter = registry.find("cursus.consumer.rebalance.count").counter();
    assertThat(counter).isNotNull();
    assertThat(counter.count()).isEqualTo(1.0);
  }

  @Test
  void recordCommitIncrementsCounter() {
    metrics.recordCommit();
    Counter counter = registry.find("cursus.consumer.commit.count").counter();
    assertThat(counter).isNotNull();
    assertThat(counter.count()).isEqualTo(1.0);
  }

  @Test
  void recordCommitFailureIncrementsCounter() {
    metrics.recordCommitFailure();
    Counter counter = registry.find("cursus.consumer.commit.failed").counter();
    assertThat(counter).isNotNull();
    assertThat(counter.count()).isEqualTo(1.0);
  }

  @Test
  void recordHeartbeatFailureIncrementsCounter() {
    metrics.recordHeartbeatFailure();
    Counter counter = registry.find("cursus.consumer.heartbeat.failed").counter();
    assertThat(counter).isNotNull();
    assertThat(counter.count()).isEqualTo(1.0);
  }

  @Test
  void registerPartitionLagGauge() {
    metrics.registerPartitionLag(0, () -> 42);
    assertThat(registry.find("cursus.consumer.partition.lag").tag("partition", "0").gauge().value())
        .isEqualTo(42.0);
  }
}
