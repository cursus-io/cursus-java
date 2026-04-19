package io.cursus.client.util;

import java.nio.charset.StandardCharsets;

/**
 * FNV-1a hash for partition key routing. Matches the Go SDK's hash/fnv usage for partition
 * assignment.
 */
public final class FnvHash {

  private static final int FNV_OFFSET_BASIS_32 = 0x811c9dc5;
  private static final int FNV_PRIME_32 = 0x01000193;

  private FnvHash() {}

  public static int fnv1a(String key) {
    byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
    int hash = FNV_OFFSET_BASIS_32;
    for (byte b : bytes) {
      hash ^= (b & 0xff);
      hash *= FNV_PRIME_32;
    }
    return hash;
  }

  public static int partition(String key, int partitionCount) {
    return (fnv1a(key) & 0x7FFFFFFF) % partitionCount;
  }
}
