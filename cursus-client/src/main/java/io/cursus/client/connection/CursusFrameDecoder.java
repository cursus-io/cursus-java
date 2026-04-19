package io.cursus.client.connection;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.nio.ByteOrder;

/**
 * Decodes Cursus TCP frames using a 4-byte big-endian length prefix. Max frame size: 64MB (matching
 * Go SDK's MAX_MESSAGE_SIZE).
 */
public class CursusFrameDecoder extends LengthFieldBasedFrameDecoder {

  private static final int MAX_FRAME_LENGTH = 64 * 1024 * 1024;
  private static final int LENGTH_FIELD_OFFSET = 0;
  private static final int LENGTH_FIELD_LENGTH = 4;
  private static final int LENGTH_ADJUSTMENT = 0;
  private static final int INITIAL_BYTES_TO_STRIP = 4;

  public CursusFrameDecoder() {
    super(
        ByteOrder.BIG_ENDIAN,
        MAX_FRAME_LENGTH,
        LENGTH_FIELD_OFFSET,
        LENGTH_FIELD_LENGTH,
        LENGTH_ADJUSTMENT,
        INITIAL_BYTES_TO_STRIP,
        true);
  }
}
