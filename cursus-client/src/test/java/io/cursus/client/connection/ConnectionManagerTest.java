package io.cursus.client.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class ConnectionManagerTest {

  @Test
  void parsesHostAndPort() {
    ConnectionManager.BrokerAddress addr = ConnectionManager.BrokerAddress.parse("localhost:9000");
    assertThat(addr.host()).isEqualTo("localhost");
    assertThat(addr.port()).isEqualTo(9000);
  }

  @Test
  void parsesMultipleBrokers() {
    List<ConnectionManager.BrokerAddress> addrs =
        ConnectionManager.BrokerAddress.parseAll(List.of("broker1:9000", "broker2:9001"));
    assertThat(addrs).hasSize(2);
    assertThat(addrs.get(0).host()).isEqualTo("broker1");
    assertThat(addrs.get(1).port()).isEqualTo(9001);
  }

  @Test
  void defaultPortWhenMissing() {
    ConnectionManager.BrokerAddress addr = ConnectionManager.BrokerAddress.parse("localhost");
    assertThat(addr.port()).isEqualTo(9000);
  }
}
