package io.cursus.client.config;

/** Policy used when a committed or requested consumer offset is outside broker retention. */
public enum AutoOffsetReset {
  /** Resume at the broker-reported earliest retained offset. */
  EARLIEST,
  /** Resume at the broker-reported latest offset. */
  LATEST,
  /** Fail the consumer instead of silently skipping or replaying records. */
  ERROR
}
