package io.cursus.client.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.cursus.client.message.CursusMessage;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class WireProtocolTest {

  @Test
  void crc32cMatchesCastagnoliReferenceVector() {
    assertThat(WireProtocol.crc32c("123456789".getBytes(StandardCharsets.UTF_8)))
        .isEqualTo(0xE3069283L);
  }

  @Test
  void negotiationPayloadMatchesGoWireV2Layout() {
    byte[] payload =
        WireProtocol.encodeNegotiationRequest(
            List.of(WireProtocol.Compression.GZIP, WireProtocol.Compression.NONE));

    assertThat(payload).containsExactly(0, 2, 0, 2, 0, 2, 1, 0);
    assertThat(WireProtocol.decodeNegotiationResponse(new byte[] {0, 2, 1}))
        .isEqualTo(WireProtocol.Compression.GZIP);
  }

  @Test
  void commandRequestUsesCommandHeaderAndCrq2Payload() {
    WireProtocol.Request request =
        WireProtocol.encodeRequest("METADATA topic=orders".getBytes(StandardCharsets.UTF_8));

    assertThat(request.command()).isEqualTo(WireProtocol.Command.METADATA);
    ByteBuffer payload = ByteBuffer.wrap(request.payload()).order(ByteOrder.BIG_ENDIAN);
    assertThat(payload.getInt()).isEqualTo(0x43525132);
    assertThat(payload.getShort()).isEqualTo((short) 2);
  }

  @Test
  void frameRoundTripValidatesChecksumAndCorrelationFields() {
    WireProtocol.Frame frame =
        new WireProtocol.Frame(
            WireProtocol.Kind.REQUEST,
            WireProtocol.Command.METADATA,
            WireProtocol.Status.NONE,
            7,
            "payload".getBytes(StandardCharsets.UTF_8));
    byte[] encoded = WireProtocol.encodeFrame(frame, WireProtocol.Compression.NONE);

    WireProtocol.Frame decoded = WireProtocol.decodeFrame(encoded, WireProtocol.Compression.NONE);
    assertThat(decoded.requestId()).isEqualTo(7);
    assertThat(decoded.command()).isEqualTo(WireProtocol.Command.METADATA);
    assertThat(decoded.payload()).containsExactly("payload".getBytes(StandardCharsets.UTF_8));

    encoded[encoded.length - 1] ^= (byte) 0xFF;
    assertThatThrownBy(() -> WireProtocol.decodeFrame(encoded, WireProtocol.Compression.NONE))
        .hasMessageContaining("checksum");
  }

  @Test
  void structuredErrorRoundTripPreservesRetryContract() {
    byte[] encoded =
        WireProtocol.encodeError(
            new WireProtocol.ErrorPayload(
                "replication_unavailable",
                WireProtocol.ErrorClass.AVAILABILITY,
                true,
                "",
                Map.of("offset", "7", "reason", "replica timeout")));

    WireProtocol.ErrorPayload decoded = WireProtocol.decodeError(encoded);
    assertThat(decoded.code()).isEqualTo("replication_unavailable");
    assertThat(decoded.errorClass()).isEqualTo(WireProtocol.ErrorClass.AVAILABILITY);
    assertThat(decoded.retryable()).isTrue();
    assertThat(decoded.fields()).containsEntry("offset", "7");
  }

  @Test
  void zeroAckBatchSuppressesWireResponse() {
    byte[] batch =
        ProtocolEncoder.encodeBatchMessages(
            "orders",
            0,
            List.of(CursusMessage.builder().seqNum(1).payload("created").build()),
            "0",
            false,
            1);

    assertThat(WireProtocol.encodeRequest(batch).responseSuppressed()).isTrue();
  }

  static Stream<WireProtocol.Compression> compressions() {
    return Stream.of(WireProtocol.Compression.values());
  }

  @ParameterizedTest
  @MethodSource("compressions")
  void allWireCompressionsRoundTrip(WireProtocol.Compression compression) {
    byte[] payload = new byte[64 * 1024];
    for (int index = 0; index < payload.length; index++) payload[index] = (byte) index;
    WireProtocol.Frame frame =
        new WireProtocol.Frame(
            WireProtocol.Kind.REQUEST,
            WireProtocol.Command.PUBLISH,
            WireProtocol.Status.NONE,
            17,
            payload);

    WireProtocol.Frame decoded =
        WireProtocol.decodeFrame(WireProtocol.encodeFrame(frame, compression), compression);
    assertThat(decoded.kind()).isEqualTo(frame.kind());
    assertThat(decoded.command()).isEqualTo(frame.command());
    assertThat(decoded.status()).isEqualTo(frame.status());
    assertThat(decoded.requestId()).isEqualTo(frame.requestId());
    assertThat(decoded.payload()).containsExactly(frame.payload());
  }
}
