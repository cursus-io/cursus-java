package io.cursus.client.integration;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class E2ERequirementTest {

  @Test
  void requiredE2EDoesNotSilentlySkipWithoutABroker() {
    String required = System.getenv("CURSUS_E2E_REQUIRED");
    if ("1".equals(required) || "true".equalsIgnoreCase(required)) {
      assertThat(System.getenv("CURSUS_E2E_BROKER"))
          .as("CURSUS_E2E_BROKER must be set when CURSUS_E2E_REQUIRED is enabled")
          .isNotBlank();
    }
  }
}
