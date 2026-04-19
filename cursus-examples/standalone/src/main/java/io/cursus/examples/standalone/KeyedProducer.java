package io.cursus.examples.standalone;

import io.cursus.client.config.CursusProducerConfig;
import io.cursus.client.producer.CursusProducer;
import java.util.List;

/** Example: keyed messages for partition routing. */
public class KeyedProducer {
  public static void main(String[] args) {
    CursusProducerConfig config =
        CursusProducerConfig.builder()
            .brokers(List.of("localhost:9000"))
            .topic("orders")
            .partitions(4)
            .batchSize(1)
            .lingerMs(0)
            .build();

    try (CursusProducer producer = new CursusProducer(config)) {
      producer.send("Order #1 placed", "customer-123");
      producer.send("Order #1 shipped", "customer-123");
      producer.send("Order #1 delivered", "customer-123");
      producer.send("Order #2 placed", "customer-456");
      producer.flush();
      System.out.println("Keyed messages sent. Acked: " + producer.getUniqueAckCount());
    }
  }
}
