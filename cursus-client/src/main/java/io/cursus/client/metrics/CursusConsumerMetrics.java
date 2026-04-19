package io.cursus.client.metrics;

import io.micrometer.core.instrument.*;
import java.time.Duration;
import java.util.function.Supplier;

public class CursusConsumerMetrics {

  private final Counter receivedCounter;
  private final Counter processedCounter;
  private final Timer processLatencyTimer;
  private final Counter rebalanceCounter;
  private final Counter commitCounter;
  private final Counter commitFailedCounter;
  private final Counter heartbeatFailedCounter;
  private final MeterRegistry registry;
  private final String topic;
  private final String group;

  public CursusConsumerMetrics(MeterRegistry registry, String topic, String group) {
    this.registry = registry;
    this.topic = topic;
    this.group = group;

    this.receivedCounter =
        Counter.builder("cursus.consumer.messages.received")
            .tag("topic", topic)
            .tag("group", group)
            .description("Total received messages")
            .register(registry);
    this.processedCounter =
        Counter.builder("cursus.consumer.messages.processed")
            .tag("topic", topic)
            .tag("group", group)
            .description("Handler-processed messages")
            .register(registry);
    this.processLatencyTimer =
        Timer.builder("cursus.consumer.process.latency")
            .tag("topic", topic)
            .tag("group", group)
            .description("Handler execution time")
            .register(registry);
    this.rebalanceCounter =
        Counter.builder("cursus.consumer.rebalance.count")
            .tag("topic", topic)
            .tag("group", group)
            .description("Rebalance events")
            .register(registry);
    this.commitCounter =
        Counter.builder("cursus.consumer.commit.count")
            .tag("topic", topic)
            .tag("group", group)
            .description("Successful offset commits")
            .register(registry);
    this.commitFailedCounter =
        Counter.builder("cursus.consumer.commit.failed")
            .tag("topic", topic)
            .tag("group", group)
            .description("Failed offset commits")
            .register(registry);
    this.heartbeatFailedCounter =
        Counter.builder("cursus.consumer.heartbeat.failed")
            .tag("topic", topic)
            .tag("group", group)
            .description("Failed heartbeats")
            .register(registry);
  }

  public void recordReceived(int count) {
    receivedCounter.increment(count);
  }

  public void recordProcessed() {
    processedCounter.increment();
  }

  public void recordProcessLatency(Duration duration) {
    processLatencyTimer.record(duration);
  }

  public void recordRebalance() {
    rebalanceCounter.increment();
  }

  public void recordCommit() {
    commitCounter.increment();
  }

  public void recordCommitFailure() {
    commitFailedCounter.increment();
  }

  public void recordHeartbeatFailure() {
    heartbeatFailedCounter.increment();
  }

  public void registerPartitionLag(int partition, Supplier<Number> supplier) {
    Gauge.builder("cursus.consumer.partition.lag", supplier)
        .tag("topic", topic)
        .tag("group", group)
        .tag("partition", String.valueOf(partition))
        .description("Partition lag (current - committed offset)")
        .register(registry);
  }
}
