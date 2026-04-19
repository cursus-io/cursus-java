package io.cursus.client.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackoffTest {

  @Test
  void firstBackoffReturnsMinDuration() {
    Backoff backoff = new Backoff(Duration.ofMillis(100), Duration.ofSeconds(10));
    Duration first = backoff.nextBackoff();
    assertThat(first.toMillis()).isBetween(100L, 110L);
  }

  @Test
  void backoffGrowsExponentially() {
    Backoff backoff = new Backoff(Duration.ofMillis(100), Duration.ofSeconds(10));
    Duration first = backoff.nextBackoff();
    Duration second = backoff.nextBackoff();
    Duration third = backoff.nextBackoff();
    assertThat(second.toMillis()).isGreaterThan(first.toMillis());
    assertThat(third.toMillis()).isGreaterThan(second.toMillis());
  }

  @Test
  void backoffCapsAtMax() {
    Backoff backoff = new Backoff(Duration.ofMillis(100), Duration.ofMillis(500));
    for (int i = 0; i < 20; i++) {
      Duration d = backoff.nextBackoff();
      assertThat(d.toMillis()).isLessThanOrEqualTo(550L);
    }
  }

  @Test
  void resetRestartsFromMin() {
    Backoff backoff = new Backoff(Duration.ofMillis(100), Duration.ofSeconds(10));
    backoff.nextBackoff();
    backoff.nextBackoff();
    backoff.nextBackoff();
    backoff.reset();
    Duration afterReset = backoff.nextBackoff();
    assertThat(afterReset.toMillis()).isBetween(100L, 110L);
  }
}
