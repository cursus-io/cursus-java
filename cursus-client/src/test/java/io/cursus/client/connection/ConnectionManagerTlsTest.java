package io.cursus.client.connection;

import static org.assertj.core.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.handler.ssl.SslContext;
import org.junit.jupiter.api.Test;

class ConnectionManagerTlsTest {

  @Test
  void buildSslContextWithNullCertPathReturnsNull() {
    SslContext ctx = ConnectionManager.buildSslContext(null);
    assertThat(ctx).isNull();
  }

  @Test
  void buildSslContextWithEmptyCertPathReturnsNull() {
    SslContext ctx = ConnectionManager.buildSslContext("");
    assertThat(ctx).isNull();
  }

  @Test
  @SuppressFBWarnings(
      value = "DMI_HARDCODED_ABSOLUTE_FILENAME",
      justification = "Intentional use of a non-existent absolute path to test error handling")
  void buildSslContextWithInvalidPathThrowsRuntimeException() {
    assertThatThrownBy(() -> ConnectionManager.buildSslContext("/nonexistent/cert.pem"))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to initialize TLS");
  }
}
