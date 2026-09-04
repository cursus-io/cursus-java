package io.cursus.client.connection;

import io.cursus.client.protocol.WireProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

/** Reassembles and validates canonical Cursus Wire v2 frames. */
public class CursusFrameDecoder extends ByteToMessageDecoder {

  private volatile WireProtocol.Compression compression = WireProtocol.Compression.NONE;

  public void setCompression(WireProtocol.Compression compression) {
    this.compression = compression;
  }

  @Override
  protected void decode(ChannelHandlerContext context, ByteBuf input, List<Object> output) {
    if (input.readableBytes() < WireProtocol.HEADER_SIZE) return;

    byte[] header = new byte[WireProtocol.HEADER_SIZE];
    input.getBytes(input.readerIndex(), header);
    int encodedSize = WireProtocol.encodedFrameSize(header);
    int frameSize = WireProtocol.HEADER_SIZE + encodedSize;
    if (input.readableBytes() < frameSize) return;

    byte[] encodedFrame = new byte[frameSize];
    input.readBytes(encodedFrame);
    output.add(WireProtocol.decodeFrame(encodedFrame, compression));
  }
}
