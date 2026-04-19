package io.cursus.client.config;

import java.time.Duration;
import java.util.List;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration for {@link io.cursus.client.consumer.CursusConsumer}. Maps to Go SDK's
 * ConsumerConfig in config.go.
 */
@Data
@Builder
public class CursusConsumerConfig {
  @Builder.Default private List<String> brokers = List.of("localhost:9000");
  private String topic;
  private String groupId;
  @Builder.Default private ConsumerMode consumerMode = ConsumerMode.STREAMING;
  @Builder.Default private Duration autoCommitInterval = Duration.ofSeconds(5);
  @Builder.Default private long sessionTimeoutMs = 30000;
  @Builder.Default private long heartbeatIntervalMs = 3000;
  @Builder.Default private int maxPollRecords = 100;
  @Builder.Default private int batchSize = 100;
  @Builder.Default private boolean immediateCommit = false;
  @Builder.Default private int commitBatchSize = 100;
  @Builder.Default private long commitIntervalMs = 5000;
  private String tlsCertPath;
  private String tlsKeyPath;
  @Builder.Default private int maxRetries = 3;
  @Builder.Default private long maxBackoffMs = 10000;
}
