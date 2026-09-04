package io.cursus.client.connection;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class CursusClientHandlerTest {

  @Test
  void streamingConsumerIsWokenWhenBrokerConnectionCloses() {
    CursusClientHandler handler = new CursusClientHandler();
    List<byte[]> pushed = new ArrayList<>();
    handler.setPushHandler(pushed::add);
    EmbeddedChannel channel = new EmbeddedChannel(handler);

    channel.close();

    assertThat(pushed).hasSize(1);
    assertThat(new String(pushed.get(0), StandardCharsets.UTF_8))
        .isEqualTo("ERROR: connection_closed class=availability retryable=true");
  }
}
