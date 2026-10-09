package io.cursus.client.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.cursus.client.exception.CursusProducerOutcomeUnknownException;
import io.cursus.client.message.AckResponse;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class CursusProducerRetryTest {

  @Test
  void retriesReplicationAvailabilityOnlyForIdempotentProducer() {
    AckResponse retryable =
        AckResponse.builder()
            .status("ERROR")
            .errorCode("replication_unavailable")
            .errorClass("availability")
            .retryable(true)
            .errorMsg("replication unavailable")
            .build();

    assertThat(CursusProducer.shouldRetry(retryable, true)).isTrue();
    assertThat(CursusProducer.shouldRetry(retryable, false)).isFalse();
  }

  @Test
  void neverRetriesBrokerClassifiedNonRetryableError() {
    AckResponse nonRetryable =
        AckResponse.builder()
            .status("ERROR")
            .errorCode("validation_failed")
            .retryable(false)
            .errorMsg("invalid request")
            .build();

    assertThat(CursusProducer.shouldRetry(nonRetryable, true)).isFalse();
  }

  @Test
  void classifiesPostSubmissionFailureAsUnknownOutcome() {
    CompletableFuture<byte[]> response = CompletableFuture.failedFuture(new IOException("closed"));

    assertThatThrownBy(() -> CursusProducer.awaitBatchResponse(response, 100, 2, "ack"))
        .isInstanceOf(CursusProducerOutcomeUnknownException.class)
        .satisfies(
            error -> {
              CursusProducerOutcomeUnknownException unknown =
                  (CursusProducerOutcomeUnknownException) error;
              assertThat(unknown.getPartition()).isEqualTo(2);
              assertThat(unknown.getStage()).isEqualTo("ack");
              assertThat(unknown.getCause()).isInstanceOf(IOException.class);
            });
  }

  @Test
  void classifiesAcknowledgementTimeoutAsUnknownOutcome() {
    CompletableFuture<byte[]> response = new CompletableFuture<>();

    assertThatThrownBy(() -> CursusProducer.awaitBatchResponse(response, 1, 0, "ack"))
        .isInstanceOf(CursusProducerOutcomeUnknownException.class);
  }
}
