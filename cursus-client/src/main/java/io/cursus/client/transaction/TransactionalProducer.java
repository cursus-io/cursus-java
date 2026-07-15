package io.cursus.client.transaction;

import io.cursus.client.protocol.BrokerCommandClient;
import io.cursus.client.protocol.CommandBuilder;
import io.cursus.client.protocol.ProtocolDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TransactionalProducer implements AutoCloseable {
  private final String transactionalId;
  private final BrokerCommandClient client;
  private final String principal;
  private final String authToken;
  private String producerId = "";
  private long epoch;
  private long seqNum;

  public TransactionalProducer(String transactionalId, List<String> brokers) {
    this(transactionalId, brokers, null, null, 5000, 3, 100);
  }

  public TransactionalProducer(
      String transactionalId,
      List<String> brokers,
      String principal,
      String authToken,
      int timeoutMs,
      int maxRetries,
      long backoffMs) {
    if (transactionalId == null || transactionalId.isBlank()) {
      throw new IllegalArgumentException("transactionalId is required");
    }
    this.transactionalId = transactionalId;
    this.client = new BrokerCommandClient(brokers, timeoutMs, maxRetries, backoffMs);
    this.principal = principal;
    this.authToken = authToken;
  }

  public ProtocolDecoder.ProducerSession initProducerId() {
    String response =
        client.sendTransaction(transactionalId, CommandBuilder.initProducerId(transactionalId));
    ProtocolDecoder.ProducerSession session = ProtocolDecoder.decodeProducerSession(response);
    this.producerId = session.producerId();
    this.epoch = session.epoch();
    this.seqNum = 0;
    return session;
  }

  public void beginTransaction() {
    ensureSession();
    client.sendTransaction(
        transactionalId, CommandBuilder.beginTxn(transactionalId, producerId, epoch));
  }

  public void publish(String topic, String message) {
    publish(topic, -1, message, "");
  }

  public void publish(String topic, int partition, String message, String key) {
    ensureSession();
    seqNum++;
    String command =
        CommandBuilder.txnPublish(
            transactionalId,
            topic,
            partition,
            producerId,
            seqNum,
            epoch,
            message,
            key,
            principal,
            authToken);
    client.sendTransaction(transactionalId, command);
  }

  public void sendOffsetsToTransaction(
      String topic, String group, String member, int generation, Map<Integer, Long> offsets) {
    ensureSession();
    if (offsets == null || offsets.isEmpty()) {
      throw new IllegalArgumentException("offsets must not be empty");
    }
    List<Integer> partitions = new ArrayList<>(offsets.keySet());
    Collections.sort(partitions);
    List<String> pairs = new ArrayList<>();
    for (Integer partition : partitions) {
      pairs.add("P" + partition + ":" + offsets.get(partition));
    }
    String command =
        CommandBuilder.sendOffsetsToTxn(
            transactionalId,
            producerId,
            epoch,
            topic,
            group,
            member,
            generation,
            String.join(",", pairs));
    client.sendTransaction(transactionalId, command);
  }

  public void commitTransaction() {
    endTransaction(true);
  }

  public void abortTransaction() {
    endTransaction(false);
  }

  public ProtocolDecoder.TransactionStatus status() {
    String response =
        client.sendTransaction(transactionalId, CommandBuilder.txnStatus(transactionalId));
    return ProtocolDecoder.decodeTransactionStatus(response);
  }

  private void endTransaction(boolean commit) {
    ensureSession();
    client.sendTransaction(
        transactionalId, CommandBuilder.endTxn(transactionalId, producerId, epoch, commit));
  }

  private void ensureSession() {
    if (producerId.isBlank()) {
      initProducerId();
    }
  }

  public String getProducerId() {
    return producerId;
  }

  public long getEpoch() {
    return epoch;
  }

  @Override
  public void close() {}
}
