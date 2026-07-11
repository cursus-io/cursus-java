package io.cursus.client.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

  @Test
  void decodeOffsetResponseOk() {
    assertThat(ProtocolDecoder.decodeOffsetResponse("OK offset=42")).isEqualTo(42);
  }

  @Test
  void decodeOffsetOutOfRangeResponse() {
    String response = "ERROR: OFFSET_OUT_OF_RANGE requested=3 earliest=10 latest=20";

    ProtocolDecoder.OffsetRange range = ProtocolDecoder.decodeOffsetOutOfRange(response);

    assertThat(ProtocolDecoder.isOffsetOutOfRange(response)).isTrue();
    assertThat(range.requested()).isEqualTo(3);
    assertThat(range.earliest()).isEqualTo(10);
    assertThat(range.latest()).isEqualTo(20);
  }

  @Test
  void decodeStreamControlOffsetOutOfRange() {
    byte[] frame =
        ("STREAM_CONTROL type=CLOSE reason=offset_out_of_range "
                + "offset=3 requested=3 earliest=10 latest=20")
            .getBytes(StandardCharsets.UTF_8);

    ProtocolDecoder.StreamControl control = ProtocolDecoder.decodeStreamControl(frame);

    assertThat(ProtocolDecoder.isStreamControlFrame(frame)).isTrue();
    assertThat(control.type()).isEqualTo("CLOSE");
    assertThat(control.reason()).isEqualTo("offset_out_of_range");
    assertThat(control.offset()).isEqualTo(3);
    assertThat(control.requested()).isEqualTo(3);
    assertThat(control.earliest()).isEqualTo(10);
    assertThat(control.latest()).isEqualTo(20);
  }

  @Test
  void zeroLengthFrameIsNotStreamControl() {
    assertThat(ProtocolDecoder.isStreamControlFrame(new byte[0])).isFalse();
  }

  @Test
  void commitFailureClassifiers() {
    assertThat(
            ProtocolDecoder.isOffsetRegression("ERROR: offset_regression current=10 attempted=9"))
        .isTrue();
    assertThat(ProtocolDecoder.isCoordinatorFailure("ERROR: GEN_MISMATCH expected=2 actual=1"))
        .isTrue();
    assertThat(ProtocolDecoder.isCoordinatorFailure("ERROR: NOT_OWNER partition=0")).isTrue();
    assertThat(ProtocolDecoder.isCoordinatorFailure("ERROR: member_not_found member=m1")).isTrue();
    assertThat(ProtocolDecoder.isCoordinatorFailure("ERROR: group_not_found group=g1")).isTrue();
    assertThat(
            ProtocolDecoder.isCoordinatorFailure("ERROR: NOT_COORDINATOR host=127.0.0.1 port=9001"))
        .isTrue();
    assertThat(ProtocolDecoder.isStaleProducerEpoch("ERROR: stale_producer_epoch producer=p1"))
        .isTrue();
  }

  @Test
  void decodeVersionResponseOk() {
    assertThat(ProtocolDecoder.decodeVersionResponse("OK version=7")).isEqualTo(7);
  }

  @Test
  void decodeSnapshotResponseOk() {
    assertThat(
            ProtocolDecoder.decodeSnapshotResponse("OK snapshot={\"version\":1,\"payload\":\"x\"}"))
        .isEqualTo("{\"version\":1,\"payload\":\"x\"}");
    assertThat(ProtocolDecoder.decodeSnapshotResponse("OK snapshot=null")).isNull();
  }

  @Test
  void strictResponseDecodersRejectLegacyValues() {
    assertThatThrownBy(() -> ProtocolDecoder.decodeOffsetResponse("42"))
        .hasMessageContaining("Unexpected offset response");
    assertThatThrownBy(() -> ProtocolDecoder.decodeVersionResponse("7"))
        .hasMessageContaining("Unexpected version response");
    assertThatThrownBy(() -> ProtocolDecoder.decodeSnapshotResponse("NULL"))
        .hasMessageContaining("Unexpected snapshot response");
    assertThatThrownBy(
            () -> ProtocolDecoder.decodeSnapshotResponse("{\"version\":1,\"payload\":\"x\"}"))
        .hasMessageContaining("Unexpected snapshot response");
  }
}
