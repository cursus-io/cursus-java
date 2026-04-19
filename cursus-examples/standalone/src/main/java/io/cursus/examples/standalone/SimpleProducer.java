package io.cursus.examples.standalone;

import io.cursus.client.config.Acks;
import io.cursus.client.config.CursusProducerConfig;
import io.cursus.client.producer.CursusProducer;
import java.util.List;

/** Minimal example: send messages to a Cursus topic. */
public class SimpleProducer {
  public static void main(String[] args) {
    CursusProducerConfig config =
        CursusProducerConfig.builder()
            .brokers(List.of("localhost:9000"))
            .topic("example-topic")
            .partitions(1)
            .acks(Acks.ONE)
            .batchSize(1)
            .lingerMs(0)
            .build();

    try (CursusProducer producer = new CursusProducer(config)) {
      for (int i = 1; i <= 10; i++) {
        long seq = producer.send("Hello Cursus #" + i);
        System.out.printf("Sent message %d (seq=%d)%n", i, seq);
      }
      producer.flush();
      System.out.println("All messages flushed. Acked: " + producer.getUniqueAckCount());
    }
  }
}
