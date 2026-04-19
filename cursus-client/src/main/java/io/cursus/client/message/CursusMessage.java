package io.cursus.client.message;

import lombok.Builder;
import lombok.Data;

/**
 * Represents a message consumed from or produced to a Cursus topic. Maps to the Go SDK's {@code
 * Message} struct in types.go.
 */
@Data
@Builder
public class CursusMessage {
  private String producerId;
  private long seqNum;
  private String payload;
  private String key;
  private long offset;
  private int epoch;
  private String eventType;
  private long schemaVersion;
  private long aggregateVersion;
  private String metadata;
  private int retryCount;
}
