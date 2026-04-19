package io.cursus.client.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RuntimeDetectorTest {

  @Test
  void detectsJavaVersion() {
    int version = RuntimeDetector.javaVersion();
    assertThat(version).isGreaterThanOrEqualTo(17);
  }

  @Test
  void virtualThreadsSupportMatchesJavaVersion() {
    boolean supports = RuntimeDetector.supportsVirtualThreads();
    int version = RuntimeDetector.javaVersion();
    if (version >= 21) {
      assertThat(supports).isTrue();
    } else {
      assertThat(supports).isFalse();
    }
  }
}
