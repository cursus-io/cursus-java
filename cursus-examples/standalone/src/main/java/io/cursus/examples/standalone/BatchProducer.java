package io.cursus.examples.standalone;

import io.cursus.client.config.CursusProducerConfig;
import io.cursus.client.producer.CursusProducer;
import java.util.List;

/** Example: batch sending with configurable batch size and linger time. */
public class BatchProducer {
  public static void main(String[] args) {
    CursusProducerConfig config =
        CursusProducerConfig.builder()
            .brokers(List.of("localhost:9000"))
            .topic("events")
            .partitions(4)
            .batchSize(500)
            .lingerMs(100)
            .compressionType("none")
            .build();

    try (CursusProducer producer = new CursusProducer(config)) {
      long start = System.currentTimeMillis();
      for (int i = 0; i < 10_000; i++) {
        producer.send("Event payload #" + i);
      }
      producer.flush();
      long elapsed = System.currentTimeMillis() - start;
      System.out.printf(
          "Sent 10,000 messages in %dms. Acked: %d%n", elapsed, producer.getUniqueAckCount());
      producer
          .getPartitionStats()
          .forEach(
              stat ->
                  System.out.printf(
                      "  Partition %d: %d pending%n", stat.partitionId(), stat.pendingCount()));
    }
  }
}
