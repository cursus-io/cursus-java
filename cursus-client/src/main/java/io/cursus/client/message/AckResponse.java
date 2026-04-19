package io.cursus.client.message;

import lombok.Builder;
import lombok.Data;

/**
 * Broker acknowledgment response for a batch of messages. Maps to the Go SDK's {@code AckResponse}
 * struct in types.go.
 */
@Data
@Builder
public class AckResponse {
  private String status;
  private long lastOffset;
  private int producerEpoch;
  private String producerId;
  private long seqStart;
  private long seqEnd;
  private String leader;
  private String errorMsg;

  public boolean isOk() {
    return "OK".equals(status);
  }

  public boolean isPartial() {
    return "PARTIAL".equals(status);
  }

  public boolean hasError() {
    return errorMsg != null && !errorMsg.isEmpty();
  }
}
