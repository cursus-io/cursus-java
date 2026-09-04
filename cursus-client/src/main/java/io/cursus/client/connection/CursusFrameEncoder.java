package io.cursus.client.connection;

import io.cursus.client.protocol.WireProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/** Encodes canonical Cursus Wire v2 frames. */
public class CursusFrameEncoder extends MessageToByteEncoder<WireProtocol.Frame> {

  private volatile WireProtocol.Compression compression = WireProtocol.Compression.NONE;

  public void setCompression(WireProtocol.Compression compression) {
    this.compression = compression;
  }

  @Override
  protected void encode(ChannelHandlerContext context, WireProtocol.Frame frame, ByteBuf output) {
    output.writeBytes(WireProtocol.encodeFrame(frame, compression));
  }
}
