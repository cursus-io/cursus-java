package io.cursus.client.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CursusProducerMetricsTest {

  private SimpleMeterRegistry registry;
  private CursusProducerMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new CursusProducerMetrics(registry, "test-topic");
  }

  @Test
  void recordSendIncrementsSentCounter() {
    metrics.recordSend(10);
    Counter counter = registry.find("cursus.producer.messages.sent").counter();
    assertThat(counter).isNotNull();
    assertThat(counter.count()).isEqualTo(10.0);
  }

  @Test
  void recordAckIncrementsAckedCounter() {
    metrics.recordAck(5);
    Counter counter = registry.find("cursus.producer.messages.acked").counter();
    assertThat(counter).isNotNull();
    assertThat(counter.count()).isEqualTo(5.0);
  }

  @Test
  void recordFailureIncrementsFailedCounter() {
    metrics.recordFailure(3);
    Counter counter = registry.find("cursus.producer.messages.failed").counter();
    assertThat(counter).isNotNull();
    assertThat(counter.count()).isEqualTo(3.0);
  }

  @Test
  void recordBatchSizeRecordsSummary() {
    metrics.recordBatchSize(50);
    assertThat(registry.find("cursus.producer.batch.size").summary()).isNotNull();
    assertThat(registry.find("cursus.producer.batch.size").summary().count()).isEqualTo(1);
  }

  @Test
  void recordSendLatencyRecordsTimer() {
    metrics.recordSendLatency(Duration.ofMillis(150));
    Timer timer = registry.find("cursus.producer.send.latency").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  void recordRetryIncrementsCounter() {
    metrics.recordRetry();
    metrics.recordRetry();
    Counter counter = registry.find("cursus.producer.retries").counter();
    assertThat(counter).isNotNull();
    assertThat(counter.count()).isEqualTo(2.0);
  }

  @Test
  void inflightGaugeReflectsSupplier() {
    metrics.registerInflightGauge(() -> 7);
    assertThat(registry.find("cursus.producer.inflight.count").gauge().value()).isEqualTo(7.0);
  }
}
