package io.cursus.client.protocol;

import io.cursus.client.connection.ConnectionManager;
import io.cursus.client.exception.CursusBrokerException;
import io.cursus.client.exception.CursusConnectionException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BrokerCommandClient {
  private final List<String> brokers;
  private final int timeoutMs;
  private final int maxRetries;
  private final long backoffMs;
  private final String tlsCertPath;
  private final String tlsKeyPath;
  private final String compressionType;
  private final String principal;
  private final String authToken;

  public BrokerCommandClient(List<String> brokers, int timeoutMs, int maxRetries, long backoffMs) {
    this(brokers, timeoutMs, maxRetries, backoffMs, "none", null, null);
  }

  public BrokerCommandClient(
      List<String> brokers,
      int timeoutMs,
      int maxRetries,
      long backoffMs,
      String compressionType,
      String principal,
      String authToken) {
    this(
        brokers,
        timeoutMs,
        maxRetries,
        backoffMs,
        null,
        null,
        compressionType,
        principal,
        authToken);
  }

  public BrokerCommandClient(
      List<String> brokers,
      int timeoutMs,
      int maxRetries,
      long backoffMs,
      String tlsCertPath,
      String tlsKeyPath,
      String compressionType,
      String principal,
      String authToken) {
    this.brokers = brokers == null || brokers.isEmpty() ? List.of("localhost:9000") : brokers;
    this.timeoutMs = timeoutMs;
    if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be non-negative");
    if ((principal == null || principal.isBlank()) != (authToken == null || authToken.isBlank())) {
      throw new IllegalArgumentException("principal and authToken must be configured together");
    }
    this.maxRetries = maxRetries;
    this.backoffMs = Math.max(0, backoffMs);
    this.tlsCertPath = tlsCertPath;
    this.tlsKeyPath = tlsKeyPath;
    this.compressionType = compressionType == null ? "none" : compressionType;
    this.principal = principal;
    this.authToken = authToken;
  }

  public String sendAny(String command, String operation) {
    return sendAny(command, operation, true);
  }

  public String sendAny(String command, String operation, boolean retryAmbiguous) {
    RuntimeException last = null;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      for (String broker : brokers) {
        try {
          String response = sendTo(broker, command);
          ProtocolDecoder.requireOk(response, operation);
          return response;
        } catch (RuntimeException e) {
          if (e instanceof CursusBrokerException brokerError) {
            if (!brokerError.isRetryable() || !retryAmbiguous) throw e;
          } else if (!retryAmbiguous) {
            throw new CursusConnectionException(
                operation + " outcome is unknown and was not retried", e);
          }
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
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
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
    try (ConnectionManager connection =
        new ConnectionManager(
            List.of(addr), tlsCertPath, tlsKeyPath, 30000, compressionType, principal, authToken)) {
      return new String(
              connection.sendCommand(command).get(timeoutMs, TimeUnit.MILLISECONDS),
              StandardCharsets.UTF_8)
          .trim();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new CursusConnectionException("interrupted during broker command", exception);
    } catch (Exception exception) {
      throw new CursusConnectionException("broker command failed", exception);
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
