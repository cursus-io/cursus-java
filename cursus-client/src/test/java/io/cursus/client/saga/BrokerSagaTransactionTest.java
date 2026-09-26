package io.cursus.client.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class BrokerSagaTransactionTest {
  @Test
  void derivesStableUuidV5CompatibleIdsForRedelivery() {
    UUID first =
        BrokerSagaTransaction.deterministicId(
            "history", "orders", "order-42", "de94b8eb-50c4-4a35-b324-59b9318af658", "1");
    UUID second =
        BrokerSagaTransaction.deterministicId(
            "history", "orders", "order-42", "de94b8eb-50c4-4a35-b324-59b9318af658", "1");

    assertThat(first).isEqualTo(second);
    assertThat(first.version()).isEqualTo(5);
  }

  @Test
  void rejectsReservedSagaTopics() {
    assertThatThrownBy(
            () ->
                new BrokerSagaTransaction.Topics(
                    "__cursus", "saga-state", "saga-commands", "saga-history"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
