package io.cursus.client.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cursus.client.message.CursusMessage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CrossLanguageConformanceTest {

  private static final List<String> CAPABILITIES =
      List.of(
          "wire_v2",
          "compression_none",
          "compression_gzip",
          "compression_snappy",
          "compression_lz4",
          "transport_tls",
          "authentication",
          "typed_errors",
          "request_correlation",
          "producer_acks",
          "producer_batching",
          "producer_idempotence",
          "safe_retry",
          "consumer_polling",
          "consumer_streaming",
          "consumer_groups",
          "offset_reset",
          "isolation_levels",
          "event_store",
          "snapshots",
          "transactions",
          "transactional_offsets",
          "transaction_status",
          "admin_client",
          "event_envelope",
          "aggregate_repository",
          "saga",
          "client_metrics",
          "error_classification");

  private static JsonNode fixture;
  private static Map<String, byte[]> vectors;

  @BeforeAll
  static void loadGoGeneratedFixture() throws IOException {
    try (InputStream stream =
        CrossLanguageConformanceTest.class.getResourceAsStream("/wire-v2.json")) {
      if (stream == null) throw new IOException("missing /wire-v2.json");
      fixture = new ObjectMapper().readTree(stream);
    }
    vectors = new HashMap<>();
    fixture
        .get("vectors")
        .forEach(
            vector ->
                vectors.put(
                    vector.get("name").asText(),
                    HexFormat.of().parseHex(vector.get("hex").asText())));
  }

  @Test
  void wireConstantsAndIdsMatchGoFixture() {
    assertThat(fixture.get("wire_version").asInt()).isEqualTo(WireProtocol.PROTOCOL_VERSION);
    assertThat(fixture.get("header_size").asInt()).isEqualTo(WireProtocol.HEADER_SIZE);
    assertThat(fixture.get("max_payload").asInt()).isEqualTo(WireProtocol.MAX_FRAME_PAYLOAD);
    for (WireProtocol.Command command : WireProtocol.Command.values()) {
      if (command != WireProtocol.Command.UNKNOWN) {
        assertThat(fixture.get("command_ids").get(command.name()).asInt())
            .isEqualTo(command.value());
      }
    }
    for (WireProtocol.Compression compression : WireProtocol.Compression.values()) {
      assertThat(fixture.get("compression_ids").get(compression.name().toLowerCase()).asInt())
          .isEqualTo(compression.value());
    }
    List<String> capabilities = new ArrayList<>();
    fixture.get("capabilities").forEach(value -> capabilities.add(value.asText()));
    assertThat(capabilities).containsExactlyElementsOf(CAPABILITIES);
  }

  @Test
  void negotiationAndFrameBytesMatchGoFixture() {
    assertThat(
            WireProtocol.encodeNegotiationRequest(
                List.of(
                    WireProtocol.Compression.GZIP,
                    WireProtocol.Compression.SNAPPY,
                    WireProtocol.Compression.LZ4,
                    WireProtocol.Compression.NONE)))
        .containsExactly(vectors.get("negotiation_request_all_compressions"));
    assertThat(WireProtocol.decodeNegotiationResponse(vectors.get("negotiation_response_lz4")))
        .isEqualTo(WireProtocol.Compression.LZ4);
    WireProtocol.Frame frame =
        new WireProtocol.Frame(
            WireProtocol.Kind.REQUEST,
            WireProtocol.Command.PUBLISH,
            WireProtocol.Status.NONE,
            42,
            "hello".getBytes(StandardCharsets.UTF_8));
    assertThat(WireProtocol.encodeFrame(frame, WireProtocol.Compression.NONE))
        .containsExactly(vectors.get("uncompressed_publish_frame"));
  }

  @Test
  void decodesGoGeneratedFramesForEveryCompression() {
    byte[] expected =
        "cross-language-compression-cross-language-compression-cross-language-compression"
            .getBytes(StandardCharsets.UTF_8);
    for (WireProtocol.Compression compression :
        List.of(
            WireProtocol.Compression.GZIP,
            WireProtocol.Compression.SNAPPY,
            WireProtocol.Compression.LZ4)) {
      WireProtocol.Frame frame =
          WireProtocol.decodeFrame(
              vectors.get(compression.name().toLowerCase() + "_publish_frame"), compression);
      assertThat(frame.requestId()).isEqualTo(77);
      assertThat(frame.command()).isEqualTo(WireProtocol.Command.PUBLISH);
      assertThat(frame.payload()).containsExactly(expected);
    }
  }

  @Test
  void commandAndStructuredErrorBytesMatchGoFixture() {
    String command =
        "APPEND_STREAM topic=events key=aggregate-7 expectedVersion=3 "
            + "eventType=Updated schemaVersion=2 metadata={\"trace\":\"a b\"} "
            + "message={\"value\":\"x y\"}";
    WireProtocol.Request request =
        WireProtocol.encodeRequest(command.getBytes(StandardCharsets.UTF_8));
    assertThat(request.command()).isEqualTo(WireProtocol.Command.APPEND_STREAM);
    assertThat(request.payload()).containsExactly(vectors.get("append_stream_command_payload"));

    WireProtocol.ErrorPayload error =
        new WireProtocol.ErrorPayload(
            "replication_unavailable",
            WireProtocol.ErrorClass.AVAILABILITY,
            true,
            "replication quorum unavailable",
            Map.of("offset", "7", "reason", "replica timeout"));
    byte[] encoded = WireProtocol.encodeError(error);
    assertThat(encoded).containsExactly(vectors.get("structured_availability_error"));
    assertThat(WireProtocol.decodeError(encoded)).isEqualTo(error);
  }

  @Test
  void fullRecordBatchBytesAndFieldsMatchGoFixture() {
    CursusMessage message =
        CursusMessage.builder()
            .offset(7)
            .seqNum(9)
            .payload("  opaque\0한글\tpayload  ")
            .key("aggregate-7")
            .producerId("producer-1")
            .epoch(-2)
            .eventType("Updated")
            .schemaVersion(2)
            .aggregateVersion(3)
            .metadata("{\"trace\":\"a b\"}")
            .timestamp(-123)
            .transactionalId("txn-1")
            .transactionState("aborted")
            .transactionMarker("abort")
            .controlBatchType("transaction")
            .controlBatchVersion(2)
            .controlBatchCoordinatorEpoch(11)
            .controlBatchKey(new byte[] {0, 1, (byte) 0xff})
            .controlBatchValue("control-value".getBytes(StandardCharsets.UTF_8))
            .build();
    byte[] encoded =
        ProtocolEncoder.encodeBatchMessages("events", 2, List.of(message), "all", true, 9);
    assertThat(encoded).containsExactly(vectors.get("full_record_batch"));

    CursusMessage decoded = ProtocolDecoder.decodeBatchMessages(encoded).get(0);
    assertThat(decoded.getPayload()).isEqualTo(message.getPayload());
    assertThat(decoded.getProducerId()).isEqualTo("producer-1");
    assertThat(decoded.getEpoch()).isEqualTo(-2);
    assertThat(decoded.getTransactionState()).isEqualTo("aborted");
    assertThat(decoded.getControlBatchKey()).containsExactly(0, 1, (byte) 0xff);
  }
}
