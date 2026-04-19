package io.cursus.client.consumer;

import io.cursus.client.config.ConsumerMode;
import io.cursus.client.config.CursusConsumerConfig;
import io.cursus.client.connection.ConnectionManager;
import io.cursus.client.connection.CursusClientHandler;
import io.cursus.client.exception.CursusConnectionException;
import io.cursus.client.exception.CursusProtocolException;
import io.cursus.client.message.CursusMessage;
import io.cursus.client.protocol.CommandBuilder;
import io.cursus.client.protocol.ProtocolDecoder;
import io.cursus.client.util.Backoff;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages consumption from a single partition using a dedicated per-partition connection. Supports
 * both polling (CONSUME) and streaming (STREAM) modes matching the Go SDK protocol.
 *
 * <p>In polling mode, the consumer sends a CONSUME command per iteration, reads one response,
 * processes it, and sleeps before repeating.
 *
 * <p>In streaming mode, the consumer sends a single STREAM command and then continuously reads
 * server-pushed responses via the push handler on the connection.
 */
public class PartitionConsumer {

  private static final Logger log = LoggerFactory.getLogger(PartitionConsumer.class);

  private final int partitionId;
  private final CursusConsumerConfig config;
  private final ConnectionManager connectionManager;
  private final String group;
  private final String member;
  private final int generation;
  private final AtomicLong currentOffset = new AtomicLong(0);
  private final AtomicLong committedOffset = new AtomicLong(0);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean rebalanceRequired = new AtomicBoolean(false);

  public PartitionConsumer(
      int partitionId,
      CursusConsumerConfig config,
      ConnectionManager connectionManager,
      String group,
      String member,
      int generation) {
    this.partitionId = partitionId;
    this.config = config;
    this.connectionManager = connectionManager;
    this.group = group;
    this.member = member;
    this.generation = generation;
  }

  /**
   * Starts consuming from this partition. This method blocks until stopped or rebalance. It creates
   * a dedicated connection for this partition, fetches the starting offset, and enters either the
   * polling or streaming loop.
   */
  public void start(Consumer<CursusMessage> handler) {
    running.set(true);
    try {
      // Create a dedicated per-partition connection
      connectionManager.connectPartition(partitionId);

      // Fetch committed offset for this partition
      long startOffset = fetchOffset();
      currentOffset.set(startOffset);
      committedOffset.set(startOffset);
      log.info("Partition {} starting at offset {}", partitionId, startOffset);

      if (config.getConsumerMode() == ConsumerMode.STREAMING) {
        runStreamingLoop(handler);
      } else {
        runPollingLoop(handler);
      }
    } catch (Exception e) {
      if (running.get()) {
        log.error("Partition {} failed to start: {}", partitionId, e.getMessage());
      }
    }
  }

  public void stop() {
    running.set(false);
  }

  public long getCurrentOffset() {
    return currentOffset.get();
  }

  public long getCommittedOffset() {
    return committedOffset.get();
  }

  public int getPartitionId() {
    return partitionId;
  }

  public boolean isRebalanceRequired() {
    return rebalanceRequired.get();
  }

  /**
   * Fetches the committed offset for this partition from the broker using the leader connection.
   * Retries up to 3 times with backoff. Throws on failure instead of returning 0, so the caller can
   * trigger a group rejoin.
   */
  private long fetchOffset() {
    Backoff backoff =
        new Backoff(Duration.ofMillis(100), Duration.ofMillis(config.getMaxBackoffMs()));
    for (int attempt = 0; attempt < 3; attempt++) {
      try {
        String cmd = CommandBuilder.fetchOffset(config.getTopic(), partitionId, group);
        byte[] response =
            connectionManager
                .sendCommand(cmd)
                .get(config.getSessionTimeoutMs(), TimeUnit.MILLISECONDS);
        String result = new String(response, StandardCharsets.UTF_8).trim();
        if (result.isEmpty() || ProtocolDecoder.isErrorResponse(result)) {
          throw new CursusProtocolException("Fetch offset error: " + result);
        }
        return Long.parseLong(result);
      } catch (NumberFormatException e) {
        log.warn("Partition {} could not parse offset on attempt {}", partitionId, attempt + 1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CursusConnectionException(
            "Interrupted during offset fetch for partition " + partitionId, e);
      } catch (Exception e) {
        log.warn(
            "Partition {} fetch offset attempt {} failed: {}",
            partitionId,
            attempt + 1,
            e.getMessage());
      }
      if (attempt < 2) {
        try {
          Thread.sleep(backoff.nextBackoff().toMillis());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new CursusConnectionException("Interrupted during offset fetch backoff", ie);
        }
      }
    }
    throw new CursusConnectionException(
        "Failed to fetch offset for partition " + partitionId + " after 3 attempts");
  }

  /** Polling mode: send CONSUME, read one response, process messages, sleep, repeat. */
  private void runPollingLoop(Consumer<CursusMessage> handler) {
    Backoff backoff =
        new Backoff(Duration.ofMillis(100), Duration.ofMillis(config.getMaxBackoffMs()));
    while (running.get()) {
      try {
        String command =
            CommandBuilder.consume(
                config.getTopic(), partitionId, currentOffset.get(), group, generation, member);
        byte[] responseBytes =
            connectionManager
                .sendOnPartition(partitionId, command.getBytes(StandardCharsets.UTF_8))
                .get(config.getSessionTimeoutMs(), TimeUnit.MILLISECONDS);
        String response = new String(responseBytes, StandardCharsets.UTF_8);

        if (ProtocolDecoder.isErrorResponse(response)) {
          log.warn("Partition {} poll error: {}", partitionId, response);
          Thread.sleep(backoff.nextBackoff().toMillis());
          continue;
        }
        if (ProtocolDecoder.isRebalanceRequired(response)) {
          log.info("Rebalance required for partition {}", partitionId);
          rebalanceRequired.set(true);
          return;
        }

        List<CursusMessage> messages = ProtocolDecoder.decodeBatchMessages(responseBytes);
        if (messages.isEmpty()) {
          Thread.sleep(backoff.nextBackoff().toMillis());
          continue;
        }

        backoff.reset();
        for (CursusMessage msg : messages) {
          handler.accept(msg);
          currentOffset.set(msg.getOffset() + 1);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        if (!running.get()) return;
        log.warn("Partition {} poll exception: {}", partitionId, e.getMessage());
        try {
          Thread.sleep(backoff.nextBackoff().toMillis());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  /**
   * Streaming mode: send STREAM command, then continuously receive server-pushed batches via the
   * push handler on the partition connection.
   */
  private void runStreamingLoop(Consumer<CursusMessage> handler) {
    Backoff backoff =
        new Backoff(Duration.ofMillis(100), Duration.ofMillis(config.getMaxBackoffMs()));

    // Set up a blocking queue to receive pushed data from the handler
    BlockingQueue<byte[]> pushQueue = new LinkedBlockingQueue<>();

    // Configure push handler on the partition connection
    CursusClientHandler partitionHandler = connectionManager.getPartitionHandler(partitionId);
    if (partitionHandler == null) {
      log.error("No handler found for partition {}", partitionId);
      return;
    }
    partitionHandler.setPushHandler(pushQueue::add);

    try {
      // Send STREAM command on the partition connection
      String command =
          CommandBuilder.stream(
              config.getTopic(), partitionId, group, currentOffset.get(), generation, member);
      connectionManager.sendOnPartition(partitionId, command.getBytes(StandardCharsets.UTF_8));

      while (running.get()) {
        try {
          byte[] responseBytes =
              pushQueue.poll(config.getSessionTimeoutMs(), TimeUnit.MILLISECONDS);
          if (responseBytes == null) {
            // Timeout waiting for data - continue if still running
            continue;
          }

          String response = new String(responseBytes, StandardCharsets.UTF_8);
          if (ProtocolDecoder.isRebalanceRequired(response)) {
            log.info("Rebalance required for partition {} in streaming mode", partitionId);
            rebalanceRequired.set(true);
            return;
          }

          List<CursusMessage> messages = ProtocolDecoder.decodeBatchMessages(responseBytes);
          backoff.reset();
          for (CursusMessage msg : messages) {
            handler.accept(msg);
            currentOffset.set(msg.getOffset() + 1);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (Exception e) {
          if (!running.get()) return;
          log.warn("Partition {} stream exception: {}", partitionId, e.getMessage());
          try {
            Thread.sleep(backoff.nextBackoff().toMillis());
            // Re-send STREAM command to re-establish
            String restream =
                CommandBuilder.stream(
                    config.getTopic(), partitionId, group, currentOffset.get(), generation, member);
            connectionManager.sendOnPartition(
                partitionId, restream.getBytes(StandardCharsets.UTF_8));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return;
          } catch (Exception re) {
            log.error("Failed to re-establish stream for partition {}", partitionId, re);
          }
        }
      }
    } finally {
      // Clear push handler when done
      partitionHandler.setPushHandler(null);
    }
  }

  /** Commits the current offset for this partition using COMMIT_OFFSET on the leader connection. */
  public void commitOffset() {
    long offset = currentOffset.get();
    if (offset <= committedOffset.get()) return;
    try {
      String command =
          CommandBuilder.commitOffset(
              config.getTopic(), partitionId, group, offset, generation, member);
      connectionManager.sendCommand(command).get(5000, TimeUnit.MILLISECONDS);
      committedOffset.set(offset);
    } catch (Exception e) {
      log.warn(
          "Failed to commit offset {} for partition {}: {}", offset, partitionId, e.getMessage());
    }
  }
}
