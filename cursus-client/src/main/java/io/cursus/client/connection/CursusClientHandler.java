package io.cursus.client.connection;

import io.cursus.client.exception.CursusConnectionException;
import io.cursus.client.exception.CursusProtocolException;
import io.cursus.client.protocol.WireProtocol;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Correlates Wire v2 responses by request id and routes streaming frames to a push callback. */
public class CursusClientHandler extends SimpleChannelInboundHandler<WireProtocol.Frame> {

  private static final Logger log = LoggerFactory.getLogger(CursusClientHandler.class);
  private static final byte[] CONNECTION_CLOSED =
      "ERROR: connection_closed class=availability retryable=true".getBytes(StandardCharsets.UTF_8);

  private final Map<Long, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
  private volatile Consumer<byte[]> pushHandler;
  private volatile long streamRequestId;
  private volatile WireProtocol.Command streamCommand = WireProtocol.Command.UNKNOWN;

  public void setPushHandler(Consumer<byte[]> handler) {
    this.pushHandler = handler;
    if (handler == null) clearStream();
  }

  public Consumer<byte[]> getPushHandler() {
    return pushHandler;
  }

  public CompletableFuture<WireProtocol.Frame> addPendingFrame(
      long requestId, WireProtocol.Command command) {
    CompletableFuture<WireProtocol.Frame> future = new CompletableFuture<>();
    PendingRequest previous =
        pendingRequests.putIfAbsent(requestId, new PendingRequest(command, future));
    if (previous != null) {
      throw new CursusProtocolException("Duplicate Wire v2 request id " + requestId);
    }
    return future;
  }

  public CompletableFuture<byte[]> addPendingRequest(long requestId, WireProtocol.Command command) {
    return addPendingFrame(requestId, command).thenApply(WireProtocol::responsePayload);
  }

  public void failRequest(long requestId, Throwable cause) {
    PendingRequest pending = pendingRequests.remove(requestId);
    if (pending != null) pending.future().completeExceptionally(cause);
  }

  public void registerStream(long requestId, WireProtocol.Command command) {
    if (pushHandler == null) {
      throw new CursusProtocolException("Streaming request requires a push handler");
    }
    streamRequestId = requestId;
    streamCommand = command;
  }

  public void cancelStream(long requestId) {
    if (streamRequestId == requestId) clearStream();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext context, WireProtocol.Frame frame) {
    if (frame.kind() == WireProtocol.Kind.STREAM) {
      handleStream(frame);
      return;
    }

    if (frame.kind() != WireProtocol.Kind.RESPONSE
        && frame.kind() != WireProtocol.Kind.NEGOTIATION_RESPONSE) {
      throw new CursusProtocolException("Unexpected Wire v2 server frame kind " + frame.kind());
    }

    PendingRequest pending = pendingRequests.remove(frame.requestId());
    if (pending == null) {
      log.warn("Received Wire v2 response with no pending request: id={}", frame.requestId());
      return;
    }
    if (pending.command() != frame.command()) {
      pending
          .future()
          .completeExceptionally(
              new CursusProtocolException(
                  "Wire v2 response command mismatch for request " + frame.requestId()));
      return;
    }
    pending.future().complete(frame);
  }

  private void handleStream(WireProtocol.Frame frame) {
    Consumer<byte[]> handler = pushHandler;
    if (handler == null
        || streamRequestId != frame.requestId()
        || streamCommand != frame.command()) {
      throw new CursusProtocolException(
          "Wire v2 stream correlation mismatch for request " + frame.requestId());
    }
    handler.accept(WireProtocol.responsePayload(frame));
    if (frame.status() == WireProtocol.Status.ERROR
        || frame.status() == WireProtocol.Status.STREAM_END) clearStream();
  }

  private void clearStream() {
    streamRequestId = 0;
    streamCommand = WireProtocol.Command.UNKNOWN;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
    log.error("Channel exception", cause);
    failPending(cause);
    context.close();
  }

  @Override
  public void channelInactive(ChannelHandlerContext context) {
    Consumer<byte[]> handler = pushHandler;
    if (handler != null) handler.accept(CONNECTION_CLOSED.clone());
    failPending(new CursusConnectionException("Connection closed"));
  }

  private void failPending(Throwable cause) {
    pendingRequests.forEach((id, pending) -> pending.future().completeExceptionally(cause));
    pendingRequests.clear();
    clearStream();
  }

  private record PendingRequest(
      WireProtocol.Command command, CompletableFuture<WireProtocol.Frame> future) {}
}
