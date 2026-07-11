package io.cursus.client.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import io.cursus.client.protocol.ProtocolDecoder;
import org.junit.jupiter.api.Test;

class PartitionConsumerStreamControlTest {

  @Test
  void restreamsRetryableCloseReasons() {
    assertThat(
            PartitionConsumer.shouldRestreamAfterStreamControl(
                new ProtocolDecoder.StreamControl(
                    "CLOSE", "offset_out_of_range", null, null, 10L, 20L)))
        .isTrue();
    assertThat(
            PartitionConsumer.shouldRestreamAfterStreamControl(
                new ProtocolDecoder.StreamControl("CLOSE", "timeout", null, null, null, null)))
        .isTrue();
    assertThat(
            PartitionConsumer.shouldRestreamAfterStreamControl(
                new ProtocolDecoder.StreamControl("CLOSE", "error", null, null, null, null)))
        .isTrue();
  }

  @Test
  void doesNotRestreamTerminalCloseReasons() {
    assertThat(
            PartitionConsumer.shouldRestreamAfterStreamControl(
                new ProtocolDecoder.StreamControl("CLOSE", "removed", null, null, null, null)))
        .isFalse();
    assertThat(
            PartitionConsumer.shouldRestreamAfterStreamControl(
                new ProtocolDecoder.StreamControl("CLOSE", "stopped", null, null, null, null)))
        .isFalse();
  }
}