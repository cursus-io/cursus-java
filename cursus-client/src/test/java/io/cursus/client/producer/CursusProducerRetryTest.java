package io.cursus.client.producer;

import static org.assertj.core.api.Assertions.assertThat;

import io.cursus.client.message.AckResponse;
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
}
