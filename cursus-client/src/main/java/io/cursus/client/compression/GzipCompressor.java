package io.cursus.client.compression;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Gzip compression implementation. Compatible with Go's compress/gzip package used in the Cursus
 * broker.
 */
public class GzipCompressor implements CursusCompressor {

  @Override
  public byte[] compress(byte[] data) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
    try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
      gzip.write(data);
    }
    return bos.toByteArray();
  }

  @Override
  public byte[] decompress(byte[] data) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    try (GZIPInputStream gzip = new GZIPInputStream(bis)) {
      return gzip.readAllBytes();
    }
  }

  @Override
  public String algorithmName() {
    return "gzip";
  }
}
