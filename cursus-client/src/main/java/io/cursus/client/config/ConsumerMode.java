package io.cursus.client.config;

/** Consumer delivery mode. */
public enum ConsumerMode {
  /** Pull-based polling with configurable interval. */
  POLLING,
  /** Persistent streaming connection for continuous delivery. */
  STREAMING
}
