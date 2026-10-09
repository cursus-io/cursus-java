package io.cursus.client.producer;

import io.cursus.client.config.CursusProducerConfig;
import io.cursus.client.connection.ConnectionManager;
import io.cursus.client.exception.CursusConnectionException;
import io.cursus.client.exception.CursusProducerClosedException;
import io.cursus.client.exception.CursusProducerOutcomeUnknownException;
import io.cursus.client.message.AckResponse;
import io.cursus.client.message.CursusMessage;
import io.cursus.client.metrics.CursusProducerMetrics;
import io.cursus.client.protocol.CommandBuilder;
import io.cursus.client.protocol.ProtocolDecoder;
import io.cursus.client.protocol.ProtocolEncoder;
import io.cursus.client.util.Backoff;
import io.cursus.client.util.ExecutorFactory;
import io.cursus.client.util.FnvHash;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cursus message producer with batching, compression, and idempotent delivery. */
public class CursusProducer implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(CursusProducer.class);

  private final CursusProducerConfig config;
  private final ConnectionManager connectionManager;
  private final PartitionBuffer[] partitionBuffers;
  private final ExecutorService flushExecutor;
  private final ScheduledExecutorService lingerScheduler;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean resourcesClosed = new AtomicBoolean(false);
  private final AtomicLong uniqueAckCount = new AtomicLong(0);
  private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
  private final Map<Long, BatchState> batchStates = new ConcurrentHashMap<>();
  private final Set<CompletableFuture<Void>> inflightFutures = ConcurrentHashMap.newKeySet();
  private final AtomicLong batchIdGenerator = new AtomicLong(0);
  private final AtomicReference<RuntimeException> deliveryFailure = new AtomicReference<>();
  private final Map<Integer, String> partitionLeaders = new ConcurrentHashMap<>();
  private final String producerId;
  private final int producerEpoch;
  private CursusProducerMetrics metrics;

  private static final Pattern PARTITION_COUNT_PATTERN = Pattern.compile("partitions=(\\d+)");

  public CursusProducer(CursusProducerConfig config) {
    this(config, null);
  }

  public CursusProducer(CursusProducerConfig config, Object metricsRegistry) {
    this.config = config;
    this.producerId = "java-" + UUID.randomUUID();
    this.producerEpoch = (int) (System.currentTimeMillis() / 1000L);
    this.connectionManager =
        new ConnectionManager(
            config.getBrokers(),
            config.getTlsCertPath(),
            config.getTlsKeyPath(),
            config.getLeaderStalenessMs(),
            config.getCompressionType(),
            config.getPrincipal(),
            config.getAuthToken());

    this.flushExecutor =
        ExecutorFactory.create(config.getMaxInflightRequests(), "cursus-producer-flush");

    this.lingerScheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "cursus-linger");
              t.setDaemon(true);
              return t;
            });
    if (config.getLingerMs() > 0) {
      lingerScheduler.scheduleAtFixedRate(
          this::lingerFlush, config.getLingerMs(), config.getLingerMs(), TimeUnit.MILLISECONDS);
    }

    if (config.isAutoCreateTopic()) createTopic(config.getTopic(), config.getPartitions());
    int verifiedPartitions = verifyPartitionCount(config.getTopic(), config.getPartitions());

    this.partitionBuffers = new PartitionBuffer[verifiedPartitions];
    for (int i = 0; i < verifiedPartitions; i++) {
      partitionBuffers[i] =
          new PartitionBuffer(
              i, config.getBatchSize(), config.getBufferSize(), producerId, producerEpoch);
    }

    fetchMetadata();

    for (int i = 0; i < verifiedPartitions; i++) {
      String leaderAddr = partitionLeaders.get(i);
      if (leaderAddr != null) {
        connectionManager.connectPartitionToAddress(i, leaderAddr);
      } else {
        connectionManager.connectPartition(i);
      }
    }

    log.info(
        "CursusProducer created for topic '{}' with {} partitions",
        config.getTopic(),
        verifiedPartitions);

    if (metricsRegistry != null) {
      try {
        this.metrics =
            new CursusProducerMetrics(
                (io.micrometer.core.instrument.MeterRegistry) metricsRegistry, config.getTopic());
        this.metrics.registerInflightGauge(() -> inflightFutures.size());
        for (int i = 0; i < partitionBuffers.length; i++) {
          final int idx = i;
          this.metrics.registerPartitionPendingGauge(i, () -> partitionBuffers[idx].pendingCount());
        }
      } catch (NoClassDefFoundError e) {
        this.metrics = null;
      }
    } else {
      this.metrics = null;
    }
  }

  private void createTopic(String topic, int partitions) {
    String cmd = CommandBuilder.create(topic, partitions);
    byte[] adminMsg = ProtocolEncoder.encodeMessage("admin", cmd.getBytes(StandardCharsets.UTF_8));
    try {
      byte[] response =
          connectionManager.send(adminMsg).get(config.getWriteTimeoutMs(), TimeUnit.MILLISECONDS);
      ProtocolDecoder.requireOk(new String(response, StandardCharsets.UTF_8), "topic auto-create");
      log.info("Topic '{}' created with {} partitions", topic, partitions);
    } catch (Exception e) {
      throw new IllegalStateException("Topic auto-create failed", e);
    }
  }

  private int verifyPartitionCount(String topic, int configured) {
    try {
      String cmd = CommandBuilder.list(topic);
      byte[] response = connectionManager.sendCommand(cmd).get(5000, TimeUnit.MILLISECONDS);
      String result = new String(response, StandardCharsets.UTF_8);
      Matcher m = PARTITION_COUNT_PATTERN.matcher(result);
      if (m.find()) {
        int actual = Integer.parseInt(m.group(1));
        if (actual != configured) {
          log.warn(
              "Partition count mismatch for '{}': configured={}, broker={}. Using broker value.",
              topic,
              configured,
              actual);
          return actual;
        }
      }
      return configured;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn(
          "Could not verify partition count for '{}', using configured value: {}",
          topic,
          configured);
      return configured;
    } catch (ExecutionException | TimeoutException e) {
      log.warn(
          "Could not verify partition count for '{}', using configured value: {}",
          topic,
          configured);
      return configured;
    }
  }

  private void fetchMetadata() {
    try {
      String cmd = CommandBuilder.metadata(config.getTopic());
      byte[] response = connectionManager.sendCommand(cmd).get(5000, TimeUnit.MILLISECONDS);
      String result = new String(response, StandardCharsets.UTF_8);
      if (result.startsWith("OK")) {
        for (String part : result.split("\\s+")) {
          if (part.startsWith("leaders=")) {
            String[] addresses = part.substring(8).split(",");
            for (int i = 0; i < addresses.length; i++) {
              String leaderAddress = addresses[i].trim();
              if (!leaderAddress.isEmpty()) {
                partitionLeaders.put(i, leaderAddress);
              }
            }
          }
        }
        log.info("Producer partition leaders: {}", partitionLeaders);
      }
    } catch (Exception e) {
      log.debug("Producer metadata fetch failed (non-critical): {}", e.getMessage());
    }
  }

  public long send(String payload) {
    return send(payload, null);
  }

  public long send(String payload, String key) {
    if (closed.get()) throw new CursusProducerClosedException();
    throwDeliveryFailure();

    int partition =
        key != null
            ? selectPartition(key, 0, partitionBuffers.length)
            : selectPartition(null, roundRobinCounter.getAndIncrement(), partitionBuffers.length);

    PartitionBuffer buffer = partitionBuffers[partition];
    long seqNum = buffer.add(payload, key);
    if (metrics != null) metrics.recordSend(1);

    List<CursusMessage> batch = buffer.drain();
    if (!batch.isEmpty()) submitBatch(partition, batch);

    return seqNum;
  }

  static int selectPartition(String key, int roundRobinValue, int partitionCount) {
    if (partitionCount <= 0) {
      throw new IllegalArgumentException("partitionCount must be positive");
    }
    return key != null
        ? FnvHash.partition(key, partitionCount)
        : Math.floorMod(roundRobinValue, partitionCount);
  }

  public void flush() {
    // Force-flush any remaining buffered messages
    for (int i = 0; i < partitionBuffers.length; i++) {
      List<CursusMessage> batch = partitionBuffers[i].forceFlush();
      if (!batch.isEmpty()) submitBatch(i, batch);
    }

    // Wait for ALL in-flight batches (including those submitted by send())
    List<CompletableFuture<Void>> snapshot = new ArrayList<>(inflightFutures);
    if (!snapshot.isEmpty()) {
      try {
        CompletableFuture.allOf(snapshot.toArray(new CompletableFuture[0]))
            .get(config.getFlushTimeoutMs(), TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        throw new CursusProducerOutcomeUnknownException(
            CursusProducerOutcomeUnknownException.UNKNOWN_PARTITION, "flush drain", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CursusConnectionException("Producer flush was interrupted", e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException runtime) throw runtime;
        throw new CursusConnectionException("Producer flush failed", cause);
      }
    }
    throwDeliveryFailure();
  }

  public long getUniqueAckCount() {
    return uniqueAckCount.get();
  }

  public List<PartitionStat> getPartitionStats() {
    List<PartitionStat> stats = new ArrayList<>();
    for (PartitionBuffer buf : partitionBuffers) {
      stats.add(new PartitionStat(buf.getPartitionId(), buf.pendingCount()));
    }
    return stats;
  }

  public boolean isConnected() {
    return !closed.get() && connectionManager.isConnected();
  }

  @Override
  public void close() {
    closed.set(true);
    if (resourcesClosed.compareAndSet(false, true)) {
      lingerScheduler.shutdown();
      RuntimeException failure = null;
      try {
        flush();
      } catch (RuntimeException exception) {
        failure = exception;
      } finally {
        flushExecutor.shutdown();
        try {
          flushExecutor.awaitTermination(config.getFlushTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
          if (failure == null) {
            failure = new CursusConnectionException("Producer close was interrupted", exception);
          }
        }
        connectionManager.close();
      }
      log.info("CursusProducer closed. Total acked: {}", uniqueAckCount.get());
      if (failure != null) throw failure;
    }
  }

  private void lingerFlush() {
    for (int i = 0; i < partitionBuffers.length; i++) {
      List<CursusMessage> batch = partitionBuffers[i].forceFlush();
      if (!batch.isEmpty()) submitBatch(i, batch);
    }
  }

  private CompletableFuture<Void> submitBatch(int partition, List<CursusMessage> messages) {
    long batchId = batchIdGenerator.incrementAndGet();
    BatchState state = new BatchState(batchId, messages);
    batchStates.put(batchId, state);

    CompletableFuture<Void> future =
        CompletableFuture.runAsync(
            () -> {
              long startNanos = System.nanoTime();
              RuntimeException lastFailure = null;
              Backoff backoff =
                  new Backoff(Duration.ofMillis(100), Duration.ofMillis(config.getMaxBackoffMs()));

              for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
                try {
                  byte[] encoded =
                      ProtocolEncoder.encodeBatchMessages(
                          config.getTopic(),
                          partition,
                          messages,
                          config.getAcks().protocolValue(),
                          config.isIdempotent(),
                          messages.get(0).getSeqNum());

                  CompletableFuture<byte[]> response =
                      connectionManager.sendOnPartition(partition, encoded);
                  byte[] responseBytes =
                      awaitBatchResponse(
                          response, config.getWriteTimeoutMs(), partition, "acknowledgement");

                  if ("0".equals(config.getAcks().protocolValue())) {
                    state.setAcknowledged(true);
                    batchStates.remove(batchId);
                    return;
                  }

                  AckResponse ack;
                  try {
                    ack = ProtocolDecoder.decodeAckResponse(responseBytes);
                  } catch (RuntimeException exception) {
                    throw new CursusProducerOutcomeUnknownException(
                        partition, "acknowledgement parsing", exception);
                  }
                  if (ack.isOk()) {
                    state.setAcknowledged(true);
                    uniqueAckCount.addAndGet(messages.size());
                    if (metrics != null) {
                      metrics.recordAck(messages.size());
                      metrics.recordBatchSize(messages.size());
                      metrics.recordSendLatency(Duration.ofNanos(System.nanoTime() - startNanos));
                    }
                    batchStates.remove(batchId);
                    return;
                  }

                  if (ack.hasError()) {
                    RuntimeException brokerFailure =
                        ProtocolDecoder.errorFromResponse(ack.getErrorMsg());
                    if (ProtocolDecoder.isTerminalProducerError(ack)) {
                      closed.set(true);
                      log.error(
                          "Producer fenced by terminal idempotency error: {}", ack.getErrorMsg());
                      lastFailure = brokerFailure;
                      break;
                    }
                    if (!shouldRetry(ack, config.isIdempotent())) {
                      log.error("Non-retryable batch error: {}", ack.getErrorMsg());
                      lastFailure = brokerFailure;
                      break;
                    }
                    lastFailure = brokerFailure;
                    if ("not_leader".equalsIgnoreCase(ack.getErrorCode())) {
                      String leader =
                          ack.getErrorFields() == null ? null : ack.getErrorFields().get("leader");
                      if (leader != null && !leader.isBlank()) {
                        partitionLeaders.put(partition, leader);
                        connectionManager.connectPartitionToAddress(partition, leader);
                      } else {
                        connectionManager.updateLeader(null);
                        connectionManager.connectPartition(partition);
                      }
                    }
                    log.warn("Batch error: {}", ack.getErrorMsg());
                  }
                } catch (CursusProducerOutcomeUnknownException e) {
                  lastFailure = e;
                  log.warn(
                      "Batch outcome unknown on attempt {} for partition {} during {}",
                      attempt + 1,
                      partition,
                      e.getStage());
                } catch (Exception e) {
                  lastFailure =
                      e instanceof RuntimeException runtime
                          ? runtime
                          : new CursusConnectionException("Batch send failed", e);
                  log.warn("Batch send failed on attempt {}: {}", attempt + 1, e.getMessage());
                }

                if (!config.isIdempotent()) break;
                if (attempt < config.getMaxRetries()) {
                  try {
                    Thread.sleep(backoff.nextBackoff().toMillis());
                  } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    lastFailure = new CursusConnectionException("Batch retry was interrupted", ie);
                    break;
                  }
                  if (metrics != null) metrics.recordRetry();
                }
              }
              batchStates.remove(batchId);
              log.error("Batch {} failed after {} retries", batchId, config.getMaxRetries());
              if (metrics != null) metrics.recordFailure(messages.size());
              recordDeliveryFailure(
                  lastFailure != null
                      ? lastFailure
                      : new CursusConnectionException(
                          "Batch " + batchId + " failed without a broker acknowledgement"));
            },
            flushExecutor);

    inflightFutures.add(future);
    future.whenComplete((v, ex) -> inflightFutures.remove(future));
    return future;
  }

  static boolean shouldRetry(AckResponse ack, boolean idempotent) {
    return idempotent && ack != null && ack.isRetryable();
  }

  static byte[] awaitBatchResponse(
      CompletableFuture<byte[]> response, long timeoutMs, int partition, String stage) {
    try {
      return response.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException exception) {
      throw new CursusProducerOutcomeUnknownException(partition, stage, exception);
    } catch (ExecutionException exception) {
      Throwable cause = exception.getCause() == null ? exception : exception.getCause();
      throw new CursusProducerOutcomeUnknownException(partition, stage, cause);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new CursusProducerOutcomeUnknownException(partition, stage, exception);
    }
  }

  private void recordDeliveryFailure(RuntimeException failure) {
    deliveryFailure.compareAndSet(null, failure);
  }

  private void throwDeliveryFailure() {
    RuntimeException failure = deliveryFailure.get();
    if (failure != null) throw failure;
  }

  public record PartitionStat(int partitionId, int pendingCount) {}
}
