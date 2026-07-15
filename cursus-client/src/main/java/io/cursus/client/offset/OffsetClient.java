package io.cursus.client.offset;

import io.cursus.client.protocol.BrokerCommandClient;
import io.cursus.client.protocol.CommandBuilder;
import io.cursus.client.protocol.ProtocolDecoder;
import java.util.List;

public class OffsetClient {
  private final BrokerCommandClient client;
  private final String principal;
  private final String authToken;

  public OffsetClient(List<String> brokers) {
    this(brokers, null, null, 5000, 3, 100);
  }

  public OffsetClient(
      List<String> brokers,
      String principal,
      String authToken,
      int timeoutMs,
      int maxRetries,
      long backoffMs) {
    this.client = new BrokerCommandClient(brokers, timeoutMs, maxRetries, backoffMs);
    this.principal = principal;
    this.authToken = authToken;
  }

  public List<ProtocolDecoder.PartitionOffsetRange> listOffsets(String topic) {
    String response =
        client.sendAny(
            CommandBuilder.listOffsets(topic, null, principal, authToken), "list offsets");
    return ProtocolDecoder.decodeListOffsetsResponse(response);
  }

  public List<ProtocolDecoder.PartitionOffsetRange> listOffsets(String topic, int partition) {
    String response =
        client.sendAny(
            CommandBuilder.listOffsets(topic, partition, principal, authToken), "list offsets");
    return ProtocolDecoder.decodeListOffsetsResponse(response);
  }
}
