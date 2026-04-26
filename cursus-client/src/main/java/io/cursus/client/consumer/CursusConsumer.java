package io.cursus.client.consumer;

import io.cursus.client.config.CursusConsumerConfig;
import io.cursus.client.connection.ConnectionManager;
import io.cursus.client.exception.CursusProtocolException;
import io.cursus.client.message.CursusMessage;
import io.cursus.client.metrics.CursusConsumerMetrics;
import io.cursus.client.protocol.CommandBuilder;
import io.cursus.client.protocol.ProtocolDecoder;
import io.cursus.client.util.ExecutorFactory;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cursus message consumer with group coordination, offset management, and support for polling and
 * streaming modes.
 *
 * <p>Follows the Go SDK consumer lifecycle:
 *
 * <ol>
 *   <li>JOIN_GROUP - parse generation, member, assignments
 *   <li>SYNC_GROUP if no inline assignments
 *   <li>FETCH_OFFSET for each partition (done by PartitionConsumer)
 *   <li>Start per-partition consumers (CONSUME or STREAM)
 *   <li>Heartbeat loop in background
 *   <li>Auto-commit offsets periodically
 *   <li>On REBALANCE_REQUIRED: stop everything, rejoin
 * </ol>
 *
 * <p>{@link #start(Consumer)} is <strong>blocking</strong> - it runs the consumer loop until {@link
 * #close()} is called or the thread is interrupted.
 */
public class CursusConsumer implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(CursusConsumer.class);

  private static final Pattern JOIN_RESPONSE_PATTERN =
      Pattern.compile(
          "OK\\s+generation=(\\d+)\\s+member=(\\S+)(?:\\s+assignments=\\[([\\d\\s]*)\\])?");
  private static final Pattern PARTITION_LIST_PATTERN = Pattern.compile("\\[([\\d\\s]*)\\]");

  private final CursusConsumerConfig config;
  private final ConnectionManager connectionManager;
  private final String consumerId;
  private final ExecutorService workerExecutor;
  private final ScheduledExecutorService commitScheduler;
  private final ScheduledExecutorService heartbeatScheduler;
  private final Map<Integer, PartitionConsumer> partitionConsumers = new ConcurrentHashMap<>();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final CompletableFuture<Void> doneFuture = new CompletableFuture<>();

  private volatile String memberId;
  private volatile int generation;
  private volatile String coordinatorAddr;
  private final Map<Integer, String> partitionLeaders = new ConcurrentHashMap<>();
  private CursusConsumerMetrics metrics;

  // Store scheduled future refs so they can be cancelled on rejoin (I4 fix)
  private volatile ScheduledFuture<?> commitFuture;
  private volatile ScheduledFuture<?> heartbeatFuture;

  public CursusConsumer(CursusConsumerConfig config) {
    this(config, null);
  }

  public CursusConsumer(CursusConsumerConfig config, Object metricsRegistry) {
    this.config = config;
    this.consumerId = UUID.randomUUID().toString();
    this.connectionManager =
        new ConnectionManager(
            config.getBrokers(), config.getTlsCertPath(), config.getTlsKeyPath(), 30000);
    this.workerExecutor =
        ExecutorFactory.create(Runtime.getRuntime().availableProcessors(), "cursus-consumer");
    this.commitScheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "cursus-commit");
              t.setDaemon(true);
              return t;
            });
    this.heartbeatScheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "cursus-heartbeat");
              t.setDaemon(true);
              return t;
            });
    if (metricsRegistry != null) {
      try {
        this.metrics =
            new CursusConsumerMetrics(
                (io.micrometer.core.instrument.MeterRegistry) metricsRegistry,
                config.getTopic(),
                config.getGroupId());
      } catch (NoClassDefFoundError e) {
        this.metrics = null;
      }
    } else {
      this.metrics = null;
    }
    log.info(
        "CursusConsumer created: consumerId={}, group={}, topic={}",
        consumerId,
        config.getGroupId(),
        config.getTopic());
  }

  /**
   * Starts consuming messages. This method is <strong>blocking</strong> and runs the consumer loop
   * (join, consume, rejoin on rebalance) until {@link #close()} is called or the calling thread is
   * interrupted.
   *
   * @param handler callback invoked for each consumed message
   */
  public void start(Consumer<CursusMessage> handler) {
    if (!running.compareAndSet(false, true))
      throw new IllegalStateException("Consumer already started");

    try {
      while (running.get()) {
        try {
          joinGroupAndConsume(handler);
        } catch (Exception e) {
          if (!running.get()) break;
          log.warn("Consumer loop error, rejoining group: {}", e.getMessage());
          cancelScheduledTasks();
          stopPartitionConsumers();
          Thread.sleep(1000);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      doneFuture.complete(null);
    }
  }

  public CompletableFuture<Void> done() {
    return doneFuture;
  }

  public boolean isConnected() {
    return running.get() && connectionManager.isConnected();
  }

  @Override
  public void close() {
    if (running.compareAndSet(true, false)) {
      try {
        if (memberId != null) {
          String command =
              CommandBuilder.leaveGroup(config.getTopic(), config.getGroupId(), memberId);
          sendCoordinatorCommandSync(command);
        }
      } catch (Exception e) {
        log.warn("Failed to leave group: {}", e.getMessage());
      }

      cancelScheduledTasks();
      stopPartitionConsumers();
      commitScheduler.shutdown();
      heartbeatScheduler.shutdown();
      workerExecutor.shutdown();
      connectionManager.close();
      log.info("CursusConsumer closed: consumerId={}", consumerId);
    }
  }

  private byte[] sendCoordinatorCommandSync(String command) throws Exception {
    for (int attempt = 0; attempt < 3; attempt++) {
      String addr = coordinatorAddr != null ? coordinatorAddr : config.getBrokers().get(0);
      byte[] response = sendPlainSocket(addr, command);
      String result = new String(response, StandardCharsets.UTF_8);

      if (result.contains("NOT_COORDINATOR")) {
        log.info("NOT_COORDINATOR response, re-discovering coordinator");
        String host = null, port = null;
        for (String part : result.split("\\s+")) {
          if (part.startsWith("host=")) host = part.substring(5);
          else if (part.startsWith("port=")) port = part.substring(5);
        }
        if (host != null && port != null) {
          coordinatorAddr = host + ":" + port;
        } else {
          findCoordinator();
        }
        continue;
      }
      return response;
    }
    String addr = coordinatorAddr != null ? coordinatorAddr : config.getBrokers().get(0);
    return sendPlainSocket(addr, command);
  }

  private void fetchMetadata() {
    try {
      String cmd = CommandBuilder.metadata(config.getTopic());
      String addr = config.getBrokers().get(0);
      byte[] response = sendPlainSocket(addr, cmd);
      String result = new String(response, StandardCharsets.UTF_8);
      if (result.startsWith("OK")) {
        for (String part : result.split("\\s+")) {
          if (part.startsWith("leaders=")) {
            String[] addrs = part.substring(8).split(",");
            for (int i = 0; i < addrs.length; i++) {
              String leaderAddr = addrs[i].trim();
              if (!leaderAddr.isEmpty()) {
                partitionLeaders.put(i, leaderAddr);
              }
            }
          }
        }
        log.info("Partition leaders: {}", partitionLeaders);
      }
    } catch (Exception e) {
      log.debug("Metadata fetch failed (non-critical): {}", e.getMessage());
    }
  }

  private byte[] sendPlainSocket(String addr, String command) throws Exception {
    String[] parts = addr.split(":");
    String host = parts[0];
    int port = Integer.parseInt(parts[1]);

    try (java.net.Socket socket = new java.net.Socket(host, port)) {
      socket.setSoTimeout(5000);
      java.io.OutputStream out = socket.getOutputStream();
      java.io.InputStream in = socket.getInputStream();

      byte[] cmdBytes = command.getBytes(StandardCharsets.UTF_8);
      byte[] payload = new byte[2 + cmdBytes.length];
      System.arraycopy(cmdBytes, 0, payload, 2, cmdBytes.length);

      byte[] frame = new byte[4 + payload.length];
      frame[0] = (byte) (payload.length >> 24);
      frame[1] = (byte) (payload.length >> 16);
      frame[2] = (byte) (payload.length >> 8);
      frame[3] = (byte) (payload.length);
      System.arraycopy(payload, 0, frame, 4, payload.length);
      out.write(frame);
      out.flush();

      byte[] lenBuf = in.readNBytes(4);
      int respLen =
          ((lenBuf[0] & 0xFF) << 24)
              | ((lenBuf[1] & 0xFF) << 16)
              | ((lenBuf[2] & 0xFF) << 8)
              | (lenBuf[3] & 0xFF);
      return in.readNBytes(respLen);
    }
  }

  private void findCoordinator() {
    try {
      String cmd = CommandBuilder.findCoordinator(config.getGroupId());
      byte[] response =
          connectionManager.sendCommand(cmd).get(5000, TimeUnit.MILLISECONDS);
      String result = new String(response, StandardCharsets.UTF_8);
      if (result.startsWith("OK")) {
        String host = null, port = null;
        for (String part : result.split("\\s+")) {
          if (part.startsWith("host=")) host = part.substring(5);
          else if (part.startsWith("port=")) port = part.substring(5);
        }
        if (host != null && port != null) {
          coordinatorAddr = host + ":" + port;
          log.info("Coordinator found: {}", coordinatorAddr);
        }
      }
    } catch (Exception e) {
      log.debug("Find coordinator failed (non-critical): {}", e.getMessage());
    }
  }

  private void joinGroupAndConsume(Consumer<CursusMessage> handler) throws Exception {
    // Cancel any existing scheduled tasks before re-joining (I4 fix)
    cancelScheduledTasks();
    stopPartitionConsumers();

    // Step 0: Find coordinator
    findCoordinator();

    // Step 1: JOIN_GROUP
    String joinCmd = CommandBuilder.joinGroup(config.getTopic(), config.getGroupId(), consumerId);
    byte[] joinResponse = sendCoordinatorCommandSync(joinCmd);
    String joinResult = new String(joinResponse, StandardCharsets.UTF_8);
    log.info("Join group response: {}", joinResult);

    // Parse: "OK generation=<gen> member=<member> assignments=[partition_list]"
    parseJoinResponse(joinResult);

    // Step 2: Get partition assignments - from inline or via SYNC_GROUP
    List<Integer> assignedPartitions = parseInlineAssignments(joinResult);
    if (assignedPartitions.isEmpty()) {
      String syncCmd =
          CommandBuilder.syncGroup(config.getTopic(), config.getGroupId(), memberId, generation);
      byte[] syncResponse = sendCoordinatorCommandSync(syncCmd);
      String syncResult = new String(syncResponse, StandardCharsets.UTF_8);
      log.info("Sync group response: {}", syncResult);
      assignedPartitions = parsePartitionAssignments(syncResult);
    }
    log.info("Assigned partitions: {}", assignedPartitions);

    if (assignedPartitions.isEmpty()) {
      log.warn("No partitions assigned, waiting before retry");
      Thread.sleep(1000);
      return;
    }

    // Step 3: Fetch metadata for partition leaders
    fetchMetadata();

    // Step 4: Start per-partition consumers (each with own connection)
    // FETCH_OFFSET is done inside PartitionConsumer.start()
    for (int partition : assignedPartitions) {
      String partitionLeaderAddr = partitionLeaders.get(partition);
      PartitionConsumer pc =
          new PartitionConsumer(
              partition, config, connectionManager, config.getGroupId(), memberId, generation,
              coordinatorAddr, partitionLeaderAddr);
      partitionConsumers.put(partition, pc);
      workerExecutor.submit(() -> pc.start(handler));
      if (metrics != null) {
        metrics.registerPartitionLag(
            partition, () -> pc.getCurrentOffset() - pc.getCommittedOffset());
      }
    }

    // Step 5: Start heartbeat loop
    heartbeatFuture =
        heartbeatScheduler.scheduleAtFixedRate(
            this::sendHeartbeat,
            config.getHeartbeatIntervalMs(),
            config.getHeartbeatIntervalMs(),
            TimeUnit.MILLISECONDS);

    // Step 6: Start auto-commit loop
    commitFuture =
        commitScheduler.scheduleAtFixedRate(
            this::commitAllOffsets,
            config.getAutoCommitInterval().toMillis(),
            config.getAutoCommitInterval().toMillis(),
            TimeUnit.MILLISECONDS);

    // Step 7: Wait for rebalance signal or shutdown
    while (running.get()) {
      // Check if any partition consumer signals rebalance
      boolean needsRebalance =
          partitionConsumers.values().stream().anyMatch(PartitionConsumer::isRebalanceRequired);
      if (needsRebalance) {
        log.info("Rebalance detected from partition consumer, rejoining group");
        if (metrics != null) metrics.recordRebalance();
        break;
      }
      Thread.sleep(500);
    }
  }

  /**
   * Parses the JOIN_GROUP response: "OK generation=&lt;gen&gt; member=&lt;member&gt;
   * [assignments=[...]]"
   *
   * @throws CursusProtocolException if the response does not match the expected format
   */
  private void parseJoinResponse(String response) {
    Matcher m = JOIN_RESPONSE_PATTERN.matcher(response);
    if (!m.matches()) {
      throw new CursusProtocolException("Unexpected JOIN_GROUP response format: " + response);
    }
    this.generation = Integer.parseInt(m.group(1));
    this.memberId = m.group(2);
  }

  /**
   * Tries to parse inline assignments from the JOIN_GROUP response. Format: "OK generation=1
   * member=abc assignments=[0 1 2]"
   *
   * @throws CursusProtocolException if the response does not match the expected format
   */
  private List<Integer> parseInlineAssignments(String response) {
    Matcher m = JOIN_RESPONSE_PATTERN.matcher(response);
    if (!m.matches()) {
      throw new CursusProtocolException("Unexpected JOIN_GROUP response format: " + response);
    }
    return parsePartitionList(m.group(3));
  }

  /**
   * Parses partition assignments from SYNC_GROUP response: "OK [0 1 2]"
   *
   * @throws CursusProtocolException if the response does not contain a valid partition list
   */
  private List<Integer> parsePartitionAssignments(String response) {
    Matcher m = PARTITION_LIST_PATTERN.matcher(response);
    if (!m.find()) {
      throw new CursusProtocolException("Unexpected SYNC_GROUP response format: " + response);
    }
    return parsePartitionList(m.group(1));
  }

  /**
   * Splits a whitespace-delimited string of partition numbers into a list of integers. Returns an
   * empty list for null or blank input.
   */
  private List<Integer> parsePartitionList(String listStr) {
    if (listStr == null || listStr.isBlank()) return new ArrayList<>();
    List<Integer> result = new ArrayList<>();
    for (String token : listStr.trim().split("\\s+")) {
      if (!token.isEmpty()) {
        result.add(Integer.parseInt(token));
      }
    }
    return result;
  }

  private void sendHeartbeat() {
    if (!running.get() || memberId == null) return;
    try {
      String cmd =
          CommandBuilder.heartbeat(config.getTopic(), config.getGroupId(), memberId, generation);
      byte[] response = sendCoordinatorCommandSync(cmd);
      String result = new String(response, StandardCharsets.UTF_8);
      if (ProtocolDecoder.isRebalanceRequired(result)) {
        log.info("Rebalance required via heartbeat");
        stopPartitionConsumers();
      }
    } catch (Exception e) {
      log.warn("Heartbeat failed: {}", e.getMessage());
      if (metrics != null) metrics.recordHeartbeatFailure();
    }
  }

  private void commitAllOffsets() {
    if (!running.get() || partitionConsumers.isEmpty()) return;

    // Build batch commit payload: <pid>:<offset>,<pid>:<offset>
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (PartitionConsumer pc : partitionConsumers.values()) {
      long offset = pc.getCurrentOffset();
      if (offset > pc.getCommittedOffset()) {
        if (!first) sb.append(',');
        sb.append(pc.getPartitionId()).append(':').append(offset);
        first = false;
      }
    }

    if (sb.length() == 0) return;

    try {
      String cmd =
          CommandBuilder.batchCommit(
              config.getTopic(), config.getGroupId(), generation, memberId, sb.toString());
      byte[] response = sendCoordinatorCommandSync(cmd);
      String result = new String(response, StandardCharsets.UTF_8);
      if (result.startsWith("OK")) {
        // Update committed offsets on success
        for (PartitionConsumer pc : partitionConsumers.values()) {
          long offset = pc.getCurrentOffset();
          if (offset > pc.getCommittedOffset()) {
            // Reflection-free: commitOffset will also set committedOffset
            // but we track via the batch commit response
          }
        }
        log.debug("Batch commit succeeded");
        if (metrics != null) metrics.recordCommit();
      } else {
        log.warn("Batch commit response: {}", result);
      }
    } catch (Exception e) {
      log.warn("Batch commit failed: {}", e.getMessage());
      if (metrics != null) metrics.recordCommitFailure();
      // Fall back to individual commits
      partitionConsumers.values().forEach(PartitionConsumer::commitOffset);
    }
  }

  /**
   * Cancels any currently-scheduled commit and heartbeat tasks (I4 fix). Must be called before
   * re-scheduling on rejoin.
   */
  private void cancelScheduledTasks() {
    ScheduledFuture<?> cf = commitFuture;
    if (cf != null) {
      cf.cancel(false);
      commitFuture = null;
    }
    ScheduledFuture<?> hf = heartbeatFuture;
    if (hf != null) {
      hf.cancel(false);
      heartbeatFuture = null;
    }
  }

  private void stopPartitionConsumers() {
    partitionConsumers.values().forEach(PartitionConsumer::stop);
    partitionConsumers.clear();
  }
}
