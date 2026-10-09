package io.cursus.client.exception;

/**
 * A producer request may have reached the broker, but no trustworthy acknowledgement was received.
 * Non-idempotent callers must reconcile application state before attempting another publish.
 */
public class CursusProducerOutcomeUnknownException extends CursusConnectionException {
  public static final int UNKNOWN_PARTITION = -1;

  private final int partition;
  private final String stage;

  public CursusProducerOutcomeUnknownException(int partition, String stage, Throwable cause) {
    super(
        partition == UNKNOWN_PARTITION
            ? "Producer outcome is unknown during " + stage
            : "Producer outcome is unknown for partition " + partition + " during " + stage,
        cause);
    this.partition = partition;
    this.stage = stage;
  }

  public int getPartition() {
    return partition;
  }

  public String getStage() {
    return stage;
  }
}
