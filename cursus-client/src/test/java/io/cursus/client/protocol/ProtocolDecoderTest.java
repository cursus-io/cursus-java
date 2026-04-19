package io.cursus.client.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import io.cursus.client.message.AckResponse;
import io.cursus.client.message.CursusMessage;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProtocolDecoderTest {

  @Test
  void decodeAckResponseOk() {
    String json =
        "{\"status\":\"OK\",\"last_offset\":100,\"producer_epoch\":1,"
            + "\"producer_id\":\"p1\",\"seq_start\":1,\"seq_end\":10,\"leader\":\"broker-1\"}";

    AckResponse ack = ProtocolDecoder.decodeAckResponse(json.getBytes(StandardCharsets.UTF_8));
    assertThat(ack.getStatus()).isEqualTo("OK");
    assertThat(ack.isOk()).isTrue();
    assertThat(ack.getLastOffset()).isEqualTo(100);
    assertThat(ack.getProducerEpoch()).isEqualTo(1);
    assertThat(ack.getProducerId()).isEqualTo("p1");
    assertThat(ack.getSeqStart()).isEqualTo(1);
    assertThat(ack.getSeqEnd()).isEqualTo(10);
  }

  @Test
  void decodeAckResponseWithError() {
    String json = "{\"status\":\"ERROR\",\"error\":\"topic not found\"}";

    AckResponse ack = ProtocolDecoder.decodeAckResponse(json.getBytes(StandardCharsets.UTF_8));
    assertThat(ack.hasError()).isTrue();
    assertThat(ack.getErrorMsg()).isEqualTo("topic not found");
  }

  @Test
  void roundTripBatchEncodeAndDecode() {
    CursusMessage original =
        CursusMessage.builder()
            .producerId("p1")
            .seqNum(1)
            .payload("hello world")
            .key("k1")
            .offset(0)
            .epoch(1)
            .eventType("")
            .schemaVersion(0)
            .aggregateVersion(0)
            .metadata("")
            .build();

    byte[] encoded =
        ProtocolEncoder.encodeBatchMessages("topic", 0, List.of(original), "1", false, 1);

    List<CursusMessage> decoded = ProtocolDecoder.decodeBatchMessages(encoded);
    assertThat(decoded).hasSize(1);
    assertThat(decoded.get(0).getPayload()).isEqualTo("hello world");
    assertThat(decoded.get(0).getProducerId()).isEqualTo("p1");
    assertThat(decoded.get(0).getKey()).isEqualTo("k1");
    assertThat(decoded.get(0).getSeqNum()).isEqualTo(1);
  }

  @Test
  void roundTripBatchWithMultipleMessages() {
    CursusMessage msg1 =
        CursusMessage.builder()
            .producerId("p1")
            .seqNum(10)
            .payload("first")
            .key("k1")
            .offset(0)
            .epoch(1)
            .eventType("evt")
            .schemaVersion(1)
            .aggregateVersion(5)
            .metadata("meta1")
            .build();
    CursusMessage msg2 =
        CursusMessage.builder()
            .producerId("p1")
            .seqNum(11)
            .payload("second")
            .key("k2")
            .offset(1)
            .epoch(1)
            .eventType("evt")
            .schemaVersion(1)
            .aggregateVersion(6)
            .metadata("meta2")
            .build();

    byte[] encoded =
        ProtocolEncoder.encodeBatchMessages("topic", 3, List.of(msg1, msg2), "-1", true, 10);

    List<CursusMessage> decoded = ProtocolDecoder.decodeBatchMessages(encoded);
    assertThat(decoded).hasSize(2);
    assertThat(decoded.get(0).getPayload()).isEqualTo("first");
    assertThat(decoded.get(1).getPayload()).isEqualTo("second");
    assertThat(decoded.get(0).getSeqNum()).isEqualTo(10);
    assertThat(decoded.get(1).getSeqNum()).isEqualTo(11);
  }
}
