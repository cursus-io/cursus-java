package io.cursus.client.producer;

import io.cursus.client.compression.CompressionRegistry;
import io.cursus.client.compression.CursusCompressor;
import io.cursus.client.config.CursusProducerConfig;
import io.cursus.client.connection.ConnectionManager;
import io.cursus.client.exception.CursusProducerClosedException;
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
  private final CursusCompressor compressor;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicLong uniqueAckCount = new AtomicLong(0);
  private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
  private final Map<Long, BatchState> batchStates = new ConcurrentHashMap<>();
  private final Set<CompletableFuture<Void>> inflightFutures = ConcurrentHashMap.newKeySet();
  private final AtomicLong batchIdGenerator = new AtomicLong(0);
  private CursusProducerMetrics metrics;

  private static final Pattern PARTITION_COUNT_PATTERN = Pattern.compile("partitions=(\\d+)");

  public CursusProducer(CursusProducerConfig config) {
    this(config, null);
  }

  public CursusProducer(CursusProducerConfig config, Object metricsRegistry) {
    this.config = config;
    this.connectionManager =
        new ConnectionManager(
            config.getBrokers(), config.getTlsCertPath(),
            config.getTlsKeyPath(), config.getLeaderStalenessMs());

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

    if (!"none".equals(config.getCompressionType())) {
      this.compressor = CompressionRegistry.getInstance().get(config.getCompressionType());
    } else {
      this.compressor = null;
    }

    createTopic(config.getTopic(), config.getPartitions());
    int verifiedPartitions = verifyPartitionCount(config.getTopic(), config.getPartitions());

    this.partitionBuffers = new PartitionBuffer[verifiedPartitions];
    for (int i = 0; i < verifiedPartitions; i++) {
      partitionBuffers[i] = new PartitionBuffer(i, config.getBatchSize(), config.getBufferSize());
    }

    for (int i = 0; i < verifiedPartitions; i++) {
      connectionManager.connectPartition(i);
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
      connectionManager.send(adminMsg).get(config.getWriteTimeoutMs(), TimeUnit.MILLISECONDS);
      log.info("Topic '{}' created with {} partitions", topic, partitions);
    } catch (Exception e) {
      log.warn("Topic creation failed (may already exist): {}", e.getMessage());
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

  public long send(String payload) {
    return send(payload, null);
  }

  public long send(String payload, String key) {
    if (closed.get()) throw new CursusProducerClosedException();

    int partition =
        (key != null)
            ? FnvHash.partition(key, config.getPartitions())
            : roundRobinCounter.getAndIncrement() % config.getPartitions();

    PartitionBuffer buffer = partitionBuffers[partition];
    long seqNum = buffer.add(payload, key);
    if (metrics != null) metrics.recordSend(1);

    List<CursusMessage> batch = buffer.drain();
    if (!batch.isEmpty()) submitBatch(partition, batch);

    return seqNum;
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
        log.warn("Flush timed out after {}ms", config.getFlushTimeoutMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        log.error("Flush failed", e.getCause());
      }
    }
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
    if (closed.compareAndSet(false, true)) {
      lingerScheduler.shutdown();
      flush();
      flushExecutor.shutdown();
      try {
        flushExecutor.awaitTermination(config.getFlushTimeoutMs(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      connectionManager.close();
      log.info("CursusProducer closed. Total acked: {}", uniqueAckCount.get());
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

                  if (compressor != null) encoded = compressor.compress(encoded);

                  byte[] responseBytes =
                      connectionManager
                          .sendOnPartition(partition, encoded)
                          .get(config.getWriteTimeoutMs(), TimeUnit.MILLISECONDS);

                  String response = new String(responseBytes, StandardCharsets.UTF_8);

                  if (ProtocolDecoder.isNotLeaderResponse(response)) {
                    log.warn("NOT_LEADER response, retrying...");
                    connectionManager.updateLeader(null);
                    Thread.sleep(backoff.nextBackoff().toMillis());
                    if (metrics != null) metrics.recordRetry();
                    continue;
                  }

                  AckResponse ack = ProtocolDecoder.decodeAckResponse(responseBytes);
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

                  if (ack.hasError()) log.warn("Batch error: {}", ack.getErrorMsg());
                } catch (TimeoutException e) {
                  log.warn("Batch send timeout on attempt {}", attempt + 1);
                } catch (Exception e) {
                  log.warn("Batch send failed on attempt {}: {}", attempt + 1, e.getMessage());
                }

                if (attempt < config.getMaxRetries()) {
                  try {
                    Thread.sleep(backoff.nextBackoff().toMillis());
                  } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                  if (metrics != null) metrics.recordRetry();
                }
              }
              batchStates.remove(batchId);
              log.error("Batch {} failed after {} retries", batchId, config.getMaxRetries());
              if (metrics != null) metrics.recordFailure(messages.size());
            },
            flushExecutor);

    inflightFutures.add(future);
    future.whenComplete((v, ex) -> inflightFutures.remove(future));
    return future;
  }

  public record PartitionStat(int partitionId, int pendingCount) {}
}
