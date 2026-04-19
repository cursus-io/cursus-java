package io.cursus.examples.standalone;

import io.cursus.client.config.ConsumerMode;
import io.cursus.client.config.CursusConsumerConfig;
import io.cursus.client.consumer.CursusConsumer;
import java.util.List;

/** Minimal example: consume messages from a Cursus topic. */
public class SimpleConsumer {
  public static void main(String[] args) throws Exception {
    CursusConsumerConfig config =
        CursusConsumerConfig.builder()
            .brokers(List.of("localhost:9000"))
            .topic("example-topic")
            .groupId("example-group")
            .consumerMode(ConsumerMode.STREAMING)
            .build();

    CursusConsumer consumer = new CursusConsumer(config);
    Thread consumerThread =
        new Thread(
            () ->
                consumer.start(
                    message ->
                        System.out.printf(
                            "Received: offset=%d, payload=%s%n",
                            message.getOffset(), message.getPayload())));
    consumerThread.setDaemon(true);
    consumerThread.start();

    System.out.println("Consumer started. Press Enter to stop...");
    System.in.read();
    consumer.close();
    System.out.println("Consumer stopped.");
  }
}
