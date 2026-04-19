package io.cursus.client.util;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with jitter. Factor: 2.0, Jitter: up to 10% of current duration. Matches the
 * Go SDK's backoff.go implementation.
 */
public class Backoff {

  private static final double FACTOR = 2.0;
  private static final double JITTER_FRACTION = 0.1;

  private final Duration min;
  private final Duration max;
  private Duration current;

  public Backoff(Duration min, Duration max) {
    this.min = min;
    this.max = max;
    this.current = min;
  }

  public Duration nextBackoff() {
    Duration result = applyJitter(current);
    current = Duration.ofMillis(Math.min((long) (current.toMillis() * FACTOR), max.toMillis()));
    return result;
  }

  public void reset() {
    current = min;
  }

  private Duration applyJitter(Duration base) {
    long jitter =
        (long) (base.toMillis() * JITTER_FRACTION * ThreadLocalRandom.current().nextDouble());
    return Duration.ofMillis(base.toMillis() + jitter);
  }
}
