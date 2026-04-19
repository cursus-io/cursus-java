package io.cursus.client.config;

import java.util.List;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration for {@link io.cursus.client.producer.CursusProducer}. Maps to Go SDK's
 * PublisherConfig in config.go.
 */
@Data
@Builder
public class CursusProducerConfig {
  @Builder.Default private List<String> brokers = List.of("localhost:9000");
  private String topic;
  @Builder.Default private int partitions = 4;
  @Builder.Default private boolean autoCreateTopic = false;
  @Builder.Default private Acks acks = Acks.ONE;
  @Builder.Default private int batchSize = 500;
  @Builder.Default private int bufferSize = 10000;
  @Builder.Default private long lingerMs = 100;
  @Builder.Default private int maxInflightRequests = 5;
  @Builder.Default private boolean idempotent = false;
  @Builder.Default private long writeTimeoutMs = 5000;
  @Builder.Default private long flushTimeoutMs = 30000;
  @Builder.Default private long leaderStalenessMs = 30000;
  @Builder.Default private String compressionType = "none";
  private String tlsCertPath;
  private String tlsKeyPath;
  @Builder.Default private int maxRetries = 3;
  @Builder.Default private long maxBackoffMs = 10000;
}
