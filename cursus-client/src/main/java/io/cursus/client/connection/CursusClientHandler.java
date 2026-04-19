package io.cursus.client.connection;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles inbound responses from the Cursus broker. Supports two modes:
 *
 * <ul>
 *   <li>Request-response: responses are queued and matched to pending requests.
 *   <li>Push (streaming): a callback receives all inbound data directly, used for STREAM commands
 *       where the broker pushes batches continuously.
 * </ul>
 */
public class CursusClientHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private static final Logger log = LoggerFactory.getLogger(CursusClientHandler.class);

  private final ConcurrentLinkedQueue<CompletableFuture<byte[]>> pendingRequests =
      new ConcurrentLinkedQueue<>();

  private volatile Consumer<byte[]> pushHandler;

  /**
   * Sets a push handler for server-push (streaming) mode. When set, all inbound data is routed to
   * this handler instead of the pending request queue. Set to {@code null} to revert to
   * request-response mode.
   */
  public void setPushHandler(Consumer<byte[]> handler) {
    this.pushHandler = handler;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
    byte[] data = new byte[msg.readableBytes()];
    msg.readBytes(data);

    Consumer<byte[]> ph = pushHandler;
    if (ph != null) {
      ph.accept(data);
      return;
    }

    CompletableFuture<byte[]> future = pendingRequests.poll();
    if (future != null) {
      future.complete(data);
    } else {
      log.warn(
          "Received response with no pending request: {}",
          new String(data, StandardCharsets.UTF_8).substring(0, Math.min(100, data.length)));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    log.error("Channel exception", cause);
    CompletableFuture<byte[]> future;
    while ((future = pendingRequests.poll()) != null) {
      future.completeExceptionally(cause);
    }
    ctx.close();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    CompletableFuture<byte[]> future;
    while ((future = pendingRequests.poll()) != null) {
      future.completeExceptionally(
          new io.cursus.client.exception.CursusConnectionException("Connection closed"));
    }
  }

  public CompletableFuture<byte[]> addPendingRequest() {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    pendingRequests.add(future);
    return future;
  }
}
