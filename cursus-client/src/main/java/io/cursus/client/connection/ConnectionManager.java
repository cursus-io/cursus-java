package io.cursus.client.connection;

import io.cursus.client.exception.CursusConnectionException;
import io.cursus.client.exception.CursusProtocolException;
import io.cursus.client.protocol.ProtocolDecoder;
import io.cursus.client.protocol.WireProtocol;
import io.cursus.client.util.Backoff;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages negotiated Wire v2 connections to Cursus brokers with leader tracking and failover. */
public class ConnectionManager implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);
  private static final long NEGOTIATION_TIMEOUT_SECONDS = 5;

  private final List<BrokerAddress> brokerAddresses;
  private final EventLoopGroup eventLoopGroup;
  private final Bootstrap bootstrap;
  private final Map<String, ManagedConnection> connections = new ConcurrentHashMap<>();
  private final Map<Integer, ManagedConnection> partitionConnections = new ConcurrentHashMap<>();
  private final long leaderStalenessMs;
  private final SslContext sslContext;
  private final List<WireProtocol.Compression> compressionPreferences;
  private final String principal;
  private final String authToken;
  private volatile String currentLeader;
  private volatile long leaderUpdatedAt;
  private volatile boolean closed;

  public ConnectionManager(
      List<String> brokers, String tlsCertPath, String tlsKeyPath, long leaderStalenessMs) {
    this(brokers, tlsCertPath, tlsKeyPath, leaderStalenessMs, "none", null, null);
  }

  public ConnectionManager(
      List<String> brokers,
      String tlsCertPath,
      String tlsKeyPath,
      long leaderStalenessMs,
      String compressionType) {
    this(brokers, tlsCertPath, tlsKeyPath, leaderStalenessMs, compressionType, null, null);
  }

  public ConnectionManager(
      List<String> brokers,
      String tlsCertPath,
      String tlsKeyPath,
      long leaderStalenessMs,
      String compressionType,
      String principal,
      String authToken) {
    this.brokerAddresses = BrokerAddress.parseAll(brokers);
    this.leaderStalenessMs = leaderStalenessMs;
    this.sslContext = buildSslContext(tlsCertPath, tlsKeyPath);
    if ((principal == null || principal.isBlank()) != (authToken == null || authToken.isBlank())) {
      throw new IllegalArgumentException("principal and authToken must be configured together");
    }
    this.principal = principal;
    this.authToken = authToken;
    WireProtocol.Compression requested = WireProtocol.compressionFromName(compressionType);
    this.compressionPreferences =
        requested == WireProtocol.Compression.NONE
            ? List.of(WireProtocol.Compression.NONE)
            : List.of(requested, WireProtocol.Compression.NONE);
    this.eventLoopGroup = new NioEventLoopGroup(1);
    this.bootstrap =
        new Bootstrap()
            .group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true);
  }

  public CompletableFuture<byte[]> send(byte[] data) {
    if (closed) return closedFuture();
    return send(getOrConnectLeader(), data);
  }

  public CompletableFuture<byte[]> sendCommand(String command) {
    return send(command.getBytes(StandardCharsets.UTF_8));
  }

  public CompletableFuture<byte[]> sendToAddress(String address, String command) {
    if (closed) return closedFuture();
    ManagedConnection connection =
        connections.compute(
            address,
            (value, current) ->
                current != null && current.channel.isActive()
                    ? current
                    : connect(BrokerAddress.parse(value)));
    return send(connection, command.getBytes(StandardCharsets.UTF_8));
  }

  public void connectPartition(int partitionId) {
    if (closed) throw new CursusConnectionException("ConnectionManager is closed");
    BrokerAddress address = BrokerAddress.parse(resolveLeader());
    ManagedConnection connection = connect(address);
    replacePartitionConnection(partitionId, connection);
    log.info("Connected partition {} to {}:{}", partitionId, address.host(), address.port());
  }

  public void connectPartitionToAddress(int partitionId, String address) {
    if (closed) throw new CursusConnectionException("ConnectionManager is closed");
    BrokerAddress brokerAddress = BrokerAddress.parse(address);
    ManagedConnection connection = connect(brokerAddress);
    replacePartitionConnection(partitionId, connection);
    log.info(
        "Connected partition {} to leader {}:{}",
        partitionId,
        brokerAddress.host(),
        brokerAddress.port());
  }

  private void replacePartitionConnection(int partitionId, ManagedConnection connection) {
    ManagedConnection previous = partitionConnections.put(partitionId, connection);
    if (previous != null && previous.channel.isActive()) previous.channel.close();
  }

  private ManagedConnection getPartitionConnection(int partitionId) {
    ManagedConnection connection = partitionConnections.get(partitionId);
    if (connection == null) {
      throw new CursusConnectionException("No connection for partition " + partitionId);
    }
    if (!connection.channel.isActive()) {
      ManagedConnection replacement = connect(connection.address);
      if (connection.handler.getPushHandler() != null) {
        replacement.handler.setPushHandler(connection.handler.getPushHandler());
      }
      replacePartitionConnection(partitionId, replacement);
      connection = replacement;
    }
    return connection;
  }

  public CompletableFuture<byte[]> sendOnPartition(int partitionId, byte[] data) {
    if (closed) return closedFuture();
    return send(getPartitionConnection(partitionId), data);
  }

  public CompletableFuture<byte[]> sendCommandOnPartition(int partitionId, String command) {
    return sendOnPartition(partitionId, command.getBytes(StandardCharsets.UTF_8));
  }

  public void sendCommandOnPartitionOneWay(int partitionId, String command) {
    if (closed) throw new CursusConnectionException("ConnectionManager is closed");
    ManagedConnection connection = getPartitionConnection(partitionId);
    WireProtocol.Request request =
        WireProtocol.encodeRequest(command.getBytes(StandardCharsets.UTF_8));
    long requestId = connection.nextRequestId.getAndIncrement();
    connection.handler.registerStream(requestId, request.command());
    ChannelFuture write = connection.channel.writeAndFlush(requestFrame(requestId, request));
    write.addListener(
        result -> {
          if (!result.isSuccess()) {
            connection.handler.cancelStream(requestId);
          }
        });
  }

  /** Returns the handler for a partition connection, allowing push-mode setup for streaming. */
  public CursusClientHandler getPartitionHandler(int partitionId) {
    ManagedConnection connection = partitionConnections.get(partitionId);
    return connection != null ? connection.handler : null;
  }

  public void updateLeader(String leaderAddress) {
    currentLeader = leaderAddress;
    leaderUpdatedAt = System.currentTimeMillis();
    log.info("Leader updated to: {}", leaderAddress);
  }

  @Override
  public void close() {
    closed = true;
    connections.values().forEach(ManagedConnection::close);
    connections.clear();
    partitionConnections.values().forEach(ManagedConnection::close);
    partitionConnections.clear();
    eventLoopGroup.shutdownGracefully();
  }

  public boolean isConnected() {
    return !closed && connections.values().stream().anyMatch(value -> value.channel.isActive());
  }

  private CompletableFuture<byte[]> send(ManagedConnection connection, byte[] data) {
    WireProtocol.Request request = WireProtocol.encodeRequest(data);
    long requestId = connection.nextRequestId.getAndIncrement();
    WireProtocol.Frame frame = requestFrame(requestId, request);
    if (request.responseSuppressed()) {
      CompletableFuture<byte[]> written = new CompletableFuture<>();
      connection
          .channel
          .writeAndFlush(frame)
          .addListener(
              result -> {
                if (result.isSuccess()) written.complete(new byte[0]);
                else written.completeExceptionally(result.cause());
              });
      return written;
    }

    CompletableFuture<byte[]> response =
        connection.handler.addPendingRequest(requestId, request.command());
    connection
        .channel
        .writeAndFlush(frame)
        .addListener(
            result -> {
              if (!result.isSuccess()) connection.handler.failRequest(requestId, result.cause());
            });
    return response;
  }

  private static WireProtocol.Frame requestFrame(long requestId, WireProtocol.Request request) {
    return new WireProtocol.Frame(
        WireProtocol.Kind.REQUEST,
        request.command(),
        WireProtocol.Status.NONE,
        requestId,
        request.payload());
  }

  private CompletableFuture<byte[]> closedFuture() {
    return CompletableFuture.failedFuture(
        new CursusConnectionException("ConnectionManager is closed"));
  }

  private ManagedConnection getOrConnectLeader() {
    String leader = resolveLeader();
    return connections.compute(
        leader,
        (value, current) ->
            current != null && current.channel.isActive()
                ? current
                : connect(BrokerAddress.parse(value)));
  }

  private String resolveLeader() {
    if (currentLeader != null && !isLeaderStale()) return currentLeader;
    BrokerAddress first = brokerAddresses.get(0);
    return first.host() + ":" + first.port();
  }

  private boolean isLeaderStale() {
    return System.currentTimeMillis() - leaderUpdatedAt > leaderStalenessMs;
  }

  private ManagedConnection connect(BrokerAddress address) {
    Backoff backoff = new Backoff(Duration.ofMillis(100), Duration.ofSeconds(10));
    for (int attempt = 0; attempt < 3; attempt++) {
      Channel channel = null;
      try {
        CursusClientHandler handler = new CursusClientHandler();
        CursusFrameDecoder decoder = new CursusFrameDecoder();
        CursusFrameEncoder encoder = new CursusFrameEncoder();
        ChannelFuture future =
            bootstrap
                .clone()
                .handler(
                    new ChannelInitializer<SocketChannel>() {
                      @Override
                      protected void initChannel(SocketChannel channel) {
                        if (sslContext != null) {
                          channel
                              .pipeline()
                              .addFirst(
                                  "ssl",
                                  sslContext.newHandler(
                                      channel.alloc(), address.host(), address.port()));
                        }
                        channel
                            .pipeline()
                            .addLast("frameDecoder", decoder)
                            .addLast("frameEncoder", encoder)
                            .addLast("handler", handler);
                      }
                    })
                .connect(address.host(), address.port())
                .sync();
        ManagedConnection connection =
            new ManagedConnection(future.channel(), handler, encoder, decoder, address);
        channel = future.channel();
        negotiate(connection);
        authenticate(connection);
        log.info("Connected to broker {}:{} with Wire v2", address.host(), address.port());
        return connection;
      } catch (Exception exception) {
        if (channel != null) channel.close();
        log.warn(
            "Connection attempt {} to {}:{} failed: {}",
            attempt + 1,
            address.host(),
            address.port(),
            exception.getMessage());
        if (attempt < 2) waitBeforeReconnect(backoff);
      }
    }
    throw new CursusConnectionException(
        "Failed to connect to " + address.host() + ":" + address.port() + " after 3 attempts");
  }

  private void negotiate(ManagedConnection connection) throws Exception {
    CompletableFuture<WireProtocol.Frame> response =
        connection.handler.addPendingFrame(0, WireProtocol.Command.NEGOTIATE);
    WireProtocol.Frame request =
        new WireProtocol.Frame(
            WireProtocol.Kind.NEGOTIATION_REQUEST,
            WireProtocol.Command.NEGOTIATE,
            WireProtocol.Status.NONE,
            0,
            WireProtocol.encodeNegotiationRequest(compressionPreferences));
    ChannelFuture write = connection.channel.writeAndFlush(request);
    write.sync();
    WireProtocol.Frame frame = response.get(NEGOTIATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (frame.kind() != WireProtocol.Kind.NEGOTIATION_RESPONSE
        || frame.status() != WireProtocol.Status.OK) {
      throw new CursusProtocolException("Cursus broker rejected Wire v2 negotiation");
    }
    WireProtocol.Compression selected = WireProtocol.decodeNegotiationResponse(frame.payload());
    if (!compressionPreferences.contains(selected)) {
      throw new CursusProtocolException("Broker selected unrequested Wire v2 compression");
    }
    connection.encoder.setCompression(selected);
    connection.decoder.setCompression(selected);
  }

  private void authenticate(ManagedConnection connection) throws Exception {
    if (principal == null || authToken == null) return;
    String command = "AUTH principal=" + principal + " token=" + authToken;
    WireProtocol.Request request =
        WireProtocol.encodeRequest(command.getBytes(StandardCharsets.UTF_8));
    long requestId = connection.nextRequestId.getAndIncrement();
    CompletableFuture<byte[]> response =
        connection.handler.addPendingRequest(requestId, request.command());
    connection.channel.writeAndFlush(requestFrame(requestId, request)).sync();
    String value =
        new String(
            response.get(NEGOTIATION_TIMEOUT_SECONDS, TimeUnit.SECONDS), StandardCharsets.UTF_8);
    ProtocolDecoder.requireOk(value, "authentication");
  }

  private static void waitBeforeReconnect(Backoff backoff) {
    try {
      Thread.sleep(backoff.nextBackoff().toMillis());
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new CursusConnectionException("Interrupted during reconnect", exception);
    }
  }

  static SslContext buildSslContext(String tlsCertPath) {
    return buildSslContext(tlsCertPath, null);
  }

  static SslContext buildSslContext(String tlsCertPath, String tlsKeyPath) {
    if ((tlsCertPath == null || tlsCertPath.isEmpty())
        != (tlsKeyPath == null || tlsKeyPath.isEmpty())) {
      if (tlsKeyPath != null && !tlsKeyPath.isEmpty()) {
        throw new IllegalArgumentException("tlsCertPath is required with tlsKeyPath");
      }
    }
    if (tlsCertPath == null || tlsCertPath.isEmpty()) return null;
    try {
      SslContextBuilder builder = SslContextBuilder.forClient().trustManager(new File(tlsCertPath));
      if (tlsKeyPath != null && !tlsKeyPath.isEmpty()) {
        builder.keyManager(new File(tlsCertPath), new File(tlsKeyPath));
      }
      return builder.build();
    } catch (Exception exception) {
      throw new RuntimeException("Failed to initialize TLS with cert: " + tlsCertPath, exception);
    }
  }

  private static final class ManagedConnection {
    private final Channel channel;
    private final CursusClientHandler handler;
    private final CursusFrameEncoder encoder;
    private final CursusFrameDecoder decoder;
    private final BrokerAddress address;
    private final AtomicLong nextRequestId = new AtomicLong(1);

    ManagedConnection(
        Channel channel,
        CursusClientHandler handler,
        CursusFrameEncoder encoder,
        CursusFrameDecoder decoder,
        BrokerAddress address) {
      this.channel = channel;
      this.handler = handler;
      this.encoder = encoder;
      this.decoder = decoder;
      this.address = address;
    }

    void close() {
      if (channel.isActive()) channel.close();
    }
  }

  public record BrokerAddress(String host, int port) {
    public static BrokerAddress parse(String address) {
      String[] parts = address.split(":");
      String host = parts[0];
      int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9000;
      return new BrokerAddress(host, port);
    }

    public static List<BrokerAddress> parseAll(List<String> addresses) {
      return addresses.stream().map(BrokerAddress::parse).collect(Collectors.toList());
    }
  }
}
