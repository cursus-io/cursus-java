package io.cursus.client.config;

/** Acknowledgment level for producer messages. */
public enum Acks {
  /** No acknowledgment (fire-and-forget). */
  NONE("0"),
  /** Leader acknowledgment only. */
  ONE("1"),
  /** All replicas must acknowledge. */
  ALL("-1");

  private final String protocolValue;

  Acks(String protocolValue) {
    this.protocolValue = protocolValue;
  }

  /** Returns the wire protocol value (e.g., "0", "1", "-1"). */
  public String protocolValue() {
    return protocolValue;
  }
}
