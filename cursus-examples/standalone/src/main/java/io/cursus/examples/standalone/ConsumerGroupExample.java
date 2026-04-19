package io.cursus.examples.standalone;

import io.cursus.client.config.ConsumerMode;
import io.cursus.client.config.CursusConsumerConfig;
import io.cursus.client.consumer.CursusConsumer;
import java.util.List;

/** Example: multiple consumers in a group sharing partition load. */
public class ConsumerGroupExample {
  public static void main(String[] args) throws Exception {
    CursusConsumerConfig config =
        CursusConsumerConfig.builder()
            .brokers(List.of("localhost:9000"))
            .topic("events")
            .groupId("event-workers")
            .consumerMode(ConsumerMode.POLLING)
            .maxPollRecords(100)
            .build();

    CursusConsumer consumer = new CursusConsumer(config);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.out.println("Shutting down...");
                  consumer.close();
                }));

    System.out.println("Consumer group member started. Ctrl+C to stop.");
    consumer.start(
        message ->
            System.out.printf(
                "[partition=%d] offset=%d: %s%n", 0, message.getOffset(), message.getPayload()));
  }
}
