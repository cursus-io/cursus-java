package io.cursus.client.compression;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class GzipCompressorTest {

  private final GzipCompressor compressor = new GzipCompressor();

  @Test
  void compressAndDecompress() throws IOException {
    byte[] original = "hello cursus message broker".getBytes(StandardCharsets.UTF_8);
    byte[] compressed = compressor.compress(original);
    byte[] decompressed = compressor.decompress(compressed);
    assertThat(decompressed).isEqualTo(original);
  }

  @Test
  void compressedIsSmallerForRepetitiveData() throws IOException {
    byte[] data =
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".repeat(100).getBytes(StandardCharsets.UTF_8);
    byte[] compressed = compressor.compress(data);
    assertThat(compressed.length).isLessThan(data.length);
  }

  @Test
  void algorithmName() {
    assertThat(compressor.algorithmName()).isEqualTo("gzip");
  }

  @Test
  void emptyInput() throws IOException {
    byte[] compressed = compressor.compress(new byte[0]);
    byte[] decompressed = compressor.decompress(compressed);
    assertThat(decompressed).isEmpty();
  }
}
