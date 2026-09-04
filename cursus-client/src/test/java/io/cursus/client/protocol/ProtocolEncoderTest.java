package io.cursus.client.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import io.cursus.client.message.CursusMessage;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProtocolEncoderTest {

  @Test
  void encodeSingleMessage() {
    byte[] encoded =
        ProtocolEncoder.encodeMessage("test-topic", "hello".getBytes(StandardCharsets.UTF_8));
    assertThat(new String(encoded, StandardCharsets.UTF_8)).isEqualTo("hello");
  }

  @Test
  void encodeBatchHasMagicBytes() {
    CursusMessage msg =
        CursusMessage.builder()
            .producerId("p1")
            .seqNum(1)
            .payload("test payload")
            .key("")
            .offset(0)
            .epoch(1)
            .eventType("")
            .schemaVersion(0)
            .aggregateVersion(0)
            .metadata("")
            .build();

    byte[] encoded =
        ProtocolEncoder.encodeBatchMessages("test-topic", 0, List.of(msg), "1", false, 1);

    ByteBuffer buf = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
    int magic = buf.getInt();
    assertThat(magic).isEqualTo(WireProtocol.BATCH_MAGIC);
    assertThat(Short.toUnsignedInt(buf.getShort())).isEqualTo(WireProtocol.BATCH_VERSION);
  }

  @Test
  void encodeBatchPartitionAsInt32() {
    CursusMessage msg =
        CursusMessage.builder()
            .producerId("p1")
            .seqNum(1)
            .payload("data")
            .key("")
            .offset(0)
            .epoch(1)
            .eventType("")
            .schemaVersion(0)
            .aggregateVersion(0)
            .metadata("")
            .build();

    byte[] encoded = ProtocolEncoder.encodeBatchMessages("topic", 5, List.of(msg), "1", false, 1);

    ByteBuffer buf = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
    buf.getInt(); // magic
    buf.getShort(); // version
    buf.getShort(); // flags
    int topicLen = buf.getInt();
    buf.position(buf.position() + topicLen); // skip topic bytes
    int partition = buf.getInt(); // partition as int32
    assertThat(partition).isEqualTo(5);
  }

  @Test
  void encodeBatchAcksWithUint32LengthPrefix() {
    CursusMessage msg =
        CursusMessage.builder()
            .producerId("p1")
            .seqNum(1)
            .payload("data")
            .key("")
            .offset(0)
            .epoch(1)
            .eventType("")
            .schemaVersion(0)
            .aggregateVersion(0)
            .metadata("")
            .build();

    byte[] encoded = ProtocolEncoder.encodeBatchMessages("topic", 0, List.of(msg), "-1", true, 1);

    ByteBuffer buf = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
    buf.getInt(); // magic
    buf.getShort(); // version
    assertThat(Short.toUnsignedInt(buf.getShort())).isEqualTo(1); // idempotent flag
    int topicLen = buf.getInt();
    buf.position(buf.position() + topicLen); // skip topic bytes
    buf.getInt(); // partition (int32)
    int acksLen = buf.getInt();
    assertThat(acksLen).isEqualTo(2); // "-1" is 2 bytes
    byte[] acksBytes = new byte[acksLen];
    buf.get(acksBytes);
    assertThat(new String(acksBytes, StandardCharsets.UTF_8)).isEqualTo("-1");
  }

  @Test
  void encodeBatchContainsMetadata() {
    CursusMessage msg =
        CursusMessage.builder()
            .producerId("producer-1")
            .seqNum(42)
            .payload("data")
            .key("order-key")
            .offset(0)
            .epoch(1)
            .eventType("")
            .schemaVersion(0)
            .aggregateVersion(0)
            .metadata("")
            .build();

    byte[] encoded =
        ProtocolEncoder.encodeBatchMessages("my-topic", 2, List.of(msg), "-1", true, 42);

    assertThat(encoded.length).isGreaterThan(10);
  }
}
