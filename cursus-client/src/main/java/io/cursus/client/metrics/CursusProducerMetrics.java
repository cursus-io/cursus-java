package io.cursus.client.metrics;

import io.micrometer.core.instrument.*;
import java.time.Duration;
import java.util.function.Supplier;

public class CursusProducerMetrics {

  private final Counter sentCounter;
  private final Counter ackedCounter;
  private final Counter failedCounter;
  private final DistributionSummary batchSizeSummary;
  private final Timer sendLatencyTimer;
  private final Counter retryCounter;
  private final MeterRegistry registry;
  private final String topic;

  public CursusProducerMetrics(MeterRegistry registry, String topic) {
    this.registry = registry;
    this.topic = topic;

    this.sentCounter =
        Counter.builder("cursus.producer.messages.sent")
            .tag("topic", topic)
            .description("Total messages submitted to send")
            .register(registry);
    this.ackedCounter =
        Counter.builder("cursus.producer.messages.acked")
            .tag("topic", topic)
            .description("Successfully acknowledged messages")
            .register(registry);
    this.failedCounter =
        Counter.builder("cursus.producer.messages.failed")
            .tag("topic", topic)
            .description("Failed messages after retries exhausted")
            .register(registry);
    this.batchSizeSummary =
        DistributionSummary.builder("cursus.producer.batch.size")
            .tag("topic", topic)
            .description("Messages per batch")
            .register(registry);
    this.sendLatencyTimer =
        Timer.builder("cursus.producer.send.latency")
            .tag("topic", topic)
            .description("Batch send-to-ack latency")
            .register(registry);
    this.retryCounter =
        Counter.builder("cursus.producer.retries")
            .tag("topic", topic)
            .description("Total retry attempts")
            .register(registry);
  }

  public void recordSend(int count) {
    sentCounter.increment(count);
  }

  public void recordAck(int count) {
    ackedCounter.increment(count);
  }

  public void recordFailure(int count) {
    failedCounter.increment(count);
  }

  public void recordBatchSize(int size) {
    batchSizeSummary.record(size);
  }

  public void recordSendLatency(Duration duration) {
    sendLatencyTimer.record(duration);
  }

  public void recordRetry() {
    retryCounter.increment();
  }

  public void registerInflightGauge(Supplier<Number> supplier) {
    Gauge.builder("cursus.producer.inflight.count", supplier)
        .tag("topic", topic)
        .description("Current in-flight batch count")
        .register(registry);
  }

  public void registerPartitionPendingGauge(int partition, Supplier<Number> supplier) {
    Gauge.builder("cursus.producer.partition.pending", supplier)
        .tag("topic", topic)
        .tag("partition", String.valueOf(partition))
        .description("Buffered messages per partition")
        .register(registry);
  }

  public void recordCompressionRatio(double ratio) {
    Gauge.builder("cursus.producer.compression.ratio", () -> ratio)
        .tag("topic", topic)
        .description("Last observed compression ratio")
        .register(registry);
  }
}
