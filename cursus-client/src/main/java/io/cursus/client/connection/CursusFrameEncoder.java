package io.cursus.client.connection;

import io.netty.handler.codec.LengthFieldPrepender;
import java.nio.ByteOrder;

/** Prepends a 4-byte big-endian length prefix to outgoing frames. */
public class CursusFrameEncoder extends LengthFieldPrepender {

  public CursusFrameEncoder() {
    super(ByteOrder.BIG_ENDIAN, 4, 0, false);
  }
}
