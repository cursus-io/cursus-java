package io.cursus.client.compression;

import java.io.IOException;

/**
 * Service provider interface for message compression. Register custom implementations via {@link
 * CompressionRegistry#register(CursusCompressor)}.
 */
public interface CursusCompressor {
  byte[] compress(byte[] data) throws IOException;

  byte[] decompress(byte[] data) throws IOException;

  String algorithmName();
}
