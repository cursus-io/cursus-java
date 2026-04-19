package io.cursus.spring.autoconfigure;

import java.time.Duration;
import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "cursus")
public class CursusProperties {

  private List<String> brokers = List.of("localhost:9000");
  private Producer producer = new Producer();
  private Consumer consumer = new Consumer();

  @Data
  public static class Producer {
    private String topic;
    private int partitions = 4;
    private String acks = "one";
    private int batchSize = 500;
    private long lingerMs = 100;
    private boolean idempotent = false;
    private String compressionType = "none";
    private int maxInflightRequests = 5;
    private long flushTimeoutMs = 30000;
    private String tlsCertPath;
    private String tlsKeyPath;
  }

  @Data
  public static class Consumer {
    private String topic;
    private String groupId;
    private String mode = "streaming";
    private Duration autoCommitInterval = Duration.ofSeconds(5);
    private int maxPollRecords = 100;
    private long sessionTimeoutMs = 30000;
    private long heartbeatIntervalMs = 3000;
    private String tlsCertPath;
    private String tlsKeyPath;
  }
}
