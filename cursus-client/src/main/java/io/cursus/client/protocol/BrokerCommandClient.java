package io.cursus.client.protocol;

import io.cursus.client.exception.CursusConnectionException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class BrokerCommandClient {
  private final List<String> brokers;
  private final int timeoutMs;
  private final int maxRetries;
  private final long backoffMs;

  public BrokerCommandClient(List<String> brokers, int timeoutMs, int maxRetries, long backoffMs) {
    this.brokers = brokers == null || brokers.isEmpty() ? List.of("localhost:9000") : brokers;
    this.timeoutMs = timeoutMs;
    this.maxRetries = Math.max(1, maxRetries);
    this.backoffMs = Math.max(0, backoffMs);
  }

  public String sendAny(String command, String operation) {
    RuntimeException last = null;
    for (int attempt = 0; attempt < maxRetries; attempt++) {
      for (String broker : brokers) {
        try {
          String response = sendTo(broker, command);
          ProtocolDecoder.requireOk(response, operation);
          return response;
        } catch (RuntimeException e) {
          if (e instanceof io.cursus.client.exception.CursusBrokerException) throw e;
          last = e;
        }
      }
      sleep(attempt);
    }
    throw new CursusConnectionException(operation + " failed after retries", last);
  }

  public String sendTransaction(String transactionalId, String command) {
    String addr = brokers.get(0);
    String lastResponse = "";
    for (int attempt = 0; attempt < maxRetries; attempt++) {
      String response = sendTo(addr, command);
      lastResponse = response;
      String redirect = ProtocolDecoder.decodeNotCoordinator(response);
      if (redirect != null) {
        addr = redirect;
        sleep(attempt);
        continue;
      }
      ProtocolDecoder.requireOk(response, "transaction command");
      return response;
    }
    ProtocolDecoder.requireOk(lastResponse, "transaction command");
    throw new CursusConnectionException("transaction command failed after redirects: " + command);
  }

  protected String sendTo(String addr, String command) {
    String[] parts = addr.split(":");
    try (Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]))) {
      socket.setSoTimeout(timeoutMs);
      OutputStream out = socket.getOutputStream();
      InputStream in = socket.getInputStream();
      byte[] payload = ProtocolEncoder.encodeMessage("", command.getBytes(StandardCharsets.UTF_8));
      ByteBuffer frame = ByteBuffer.allocate(4 + payload.length).order(ByteOrder.BIG_ENDIAN);
      frame.putInt(payload.length);
      frame.put(payload);
      out.write(frame.array());
      out.flush();

      byte[] lenBuf = in.readNBytes(4);
      if (lenBuf.length != 4) throw new CursusConnectionException("connection closed");
      int len = ByteBuffer.wrap(lenBuf).order(ByteOrder.BIG_ENDIAN).getInt();
      byte[] response = in.readNBytes(len);
      return new String(response, StandardCharsets.UTF_8).trim();
    } catch (Exception e) {
      if (e instanceof RuntimeException runtime) throw runtime;
      throw new CursusConnectionException("command failed: " + command, e);
    }
  }

  private void sleep(int attempt) {
    if (backoffMs <= 0) return;
    try {
      Thread.sleep(Math.min(backoffMs * (1L << attempt), timeoutMs));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CursusConnectionException("interrupted during retry", e);
    }
  }
}
