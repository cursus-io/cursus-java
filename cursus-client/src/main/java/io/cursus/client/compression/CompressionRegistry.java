package io.cursus.client.compression;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for compression algorithms. Gzip is registered by default. Register additional
 * compressors via {@link #register(CursusCompressor)}.
 */
public class CompressionRegistry {

  private static final CompressionRegistry INSTANCE = new CompressionRegistry();

  private final Map<String, CursusCompressor> compressors = new ConcurrentHashMap<>();

  private CompressionRegistry() {
    register(new GzipCompressor());
  }

  public static CompressionRegistry getInstance() {
    return INSTANCE;
  }

  public void register(CursusCompressor compressor) {
    compressors.put(compressor.algorithmName(), compressor);
  }

  public CursusCompressor get(String algorithmName) {
    return compressors.get(algorithmName);
  }
}
