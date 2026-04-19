package io.cursus.client.connection;

import io.cursus.client.exception.CursusConnectionException;
import io.cursus.client.util.Backoff;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
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
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages Netty connections to Cursus brokers with leader tracking and failover. */
public class ConnectionManager implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

  private final List<BrokerAddress> brokerAddresses;
  private final EventLoopGroup eventLoopGroup;
  private final Bootstrap bootstrap;
  private final Map<String, ManagedConnection> connections = new ConcurrentHashMap<>();
  private final Map<Integer, ManagedConnection> partitionConnections = new ConcurrentHashMap<>();
  private final long leaderStalenessMs;
  private volatile String currentLeader;
  private volatile long leaderUpdatedAt;
  private volatile boolean closed;
  private final SslContext sslContext;

  public ConnectionManager(
      List<String> brokers, String tlsCertPath, String tlsKeyPath, long leaderStalenessMs) {
    this.brokerAddresses = BrokerAddress.parseAll(brokers);
    this.leaderStalenessMs = leaderStalenessMs;
    this.sslContext = buildSslContext(tlsCertPath);
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
    if (closed) {
      return CompletableFuture.failedFuture(
          new CursusConnectionException("ConnectionManager is closed"));
    }
    ManagedConnection conn = getOrConnectLeader();
    CompletableFuture<byte[]> responseFuture = conn.handler.addPendingRequest();
    conn.channel.writeAndFlush(Unpooled.wrappedBuffer(data));
    return responseFuture;
  }

  public CompletableFuture<byte[]> sendCommand(String command) {
    return send(command.getBytes(StandardCharsets.UTF_8));
  }

  public void connectPartition(int partitionId) {
    if (closed) {
      throw new CursusConnectionException("ConnectionManager is closed");
    }
    BrokerAddress addr = BrokerAddress.parse(resolveLeader());
    ManagedConnection conn = connect(addr);
    partitionConnections.put(partitionId, conn);
    log.info("Connected partition {} to {}:{}", partitionId, addr.host(), addr.port());
  }

  public ManagedConnection getPartitionConnection(int partitionId) {
    ManagedConnection conn = partitionConnections.get(partitionId);
    if (conn == null) {
      throw new CursusConnectionException("No connection for partition " + partitionId);
    }
    return conn;
  }

  public CompletableFuture<byte[]> sendOnPartition(int partitionId, byte[] data) {
    if (closed) {
      return CompletableFuture.failedFuture(
          new CursusConnectionException("ConnectionManager is closed"));
    }
    ManagedConnection conn = getPartitionConnection(partitionId);
    CompletableFuture<byte[]> responseFuture = conn.handler.addPendingRequest();
    conn.channel.writeAndFlush(Unpooled.wrappedBuffer(data));
    return responseFuture;
  }

  /** Returns the handler for a partition connection, allowing push-mode setup for streaming. */
  public CursusClientHandler getPartitionHandler(int partitionId) {
    ManagedConnection conn = partitionConnections.get(partitionId);
    return conn != null ? conn.handler : null;
  }

  public void updateLeader(String leaderAddress) {
    this.currentLeader = leaderAddress;
    this.leaderUpdatedAt = System.currentTimeMillis();
    log.info("Leader updated to: {}", leaderAddress);
  }

  @Override
  public void close() {
    closed = true;
    connections
        .values()
        .forEach(
            conn -> {
              if (conn.channel.isActive()) conn.channel.close();
            });
    connections.clear();
    partitionConnections
        .values()
        .forEach(
            conn -> {
              if (conn.channel.isActive()) conn.channel.close();
            });
    partitionConnections.clear();
    eventLoopGroup.shutdownGracefully();
  }

  public boolean isConnected() {
    if (closed) return false;
    return connections.values().stream().anyMatch(conn -> conn.channel.isActive());
  }

  private ManagedConnection getOrConnectLeader() {
    String leader = resolveLeader();
    return connections.computeIfAbsent(leader, addr -> connect(BrokerAddress.parse(addr)));
  }

  private String resolveLeader() {
    if (currentLeader != null && !isLeaderStale()) return currentLeader;
    BrokerAddress first = brokerAddresses.get(0);
    return first.host() + ":" + first.port();
  }

  private boolean isLeaderStale() {
    return System.currentTimeMillis() - leaderUpdatedAt > leaderStalenessMs;
  }

  private ManagedConnection connect(BrokerAddress addr) {
    Backoff backoff = new Backoff(Duration.ofMillis(100), Duration.ofSeconds(10));
    for (int attempt = 0; attempt < 3; attempt++) {
      try {
        CursusClientHandler handler = new CursusClientHandler();
        ChannelFuture future =
            bootstrap
                .clone()
                .handler(
                    new ChannelInitializer<SocketChannel>() {
                      @Override
                      protected void initChannel(SocketChannel ch) {
                        if (sslContext != null) {
                          ch.pipeline()
                              .addFirst(
                                  "ssl",
                                  sslContext.newHandler(ch.alloc(), addr.host(), addr.port()));
                        }
                        ch.pipeline()
                            .addLast("frameDecoder", new CursusFrameDecoder())
                            .addLast("frameEncoder", new CursusFrameEncoder())
                            .addLast("handler", handler);
                      }
                    })
                .connect(addr.host(), addr.port())
                .sync();
        log.info("Connected to broker {}:{}", addr.host(), addr.port());
        return new ManagedConnection(future.channel(), handler);
      } catch (Exception e) {
        log.warn(
            "Connection attempt {} to {}:{} failed: {}",
            attempt + 1,
            addr.host(),
            addr.port(),
            e.getMessage());
        if (attempt < 2) {
          try {
            Thread.sleep(backoff.nextBackoff().toMillis());
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new CursusConnectionException("Interrupted during reconnect", ie);
          }
        }
      }
    }
    throw new CursusConnectionException(
        "Failed to connect to " + addr.host() + ":" + addr.port() + " after 3 attempts");
  }

  static SslContext buildSslContext(String tlsCertPath) {
    if (tlsCertPath == null || tlsCertPath.isEmpty()) return null;
    try {
      return SslContextBuilder.forClient().trustManager(new File(tlsCertPath)).build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize TLS with cert: " + tlsCertPath, e);
    }
  }

  private static class ManagedConnection {
    final Channel channel;
    final CursusClientHandler handler;

    ManagedConnection(Channel channel, CursusClientHandler handler) {
      this.channel = channel;
      this.handler = handler;
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
