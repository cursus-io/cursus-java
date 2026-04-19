package io.cursus.examples.standalone;

import io.cursus.client.config.Acks;
import io.cursus.client.config.ConsumerMode;
import io.cursus.client.config.CursusConsumerConfig;
import io.cursus.client.config.CursusProducerConfig;
import io.cursus.client.consumer.CursusConsumer;
import io.cursus.client.producer.CursusProducer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** End-to-end test: produce messages, then consume them. */
public class ProduceAndConsume {

  public static void main(String[] args) throws Exception {
    String topic = "e2e-test-" + System.currentTimeMillis();
    int messageCount = 5;

    // === Produce ===
    CursusProducerConfig prodConfig =
        CursusProducerConfig.builder()
            .brokers(List.of("localhost:9000"))
            .topic(topic)
            .partitions(1)
            .acks(Acks.ONE)
            .batchSize(1)
            .lingerMs(0)
            .build();

    try (CursusProducer producer = new CursusProducer(prodConfig)) {
      for (int i = 1; i <= messageCount; i++) {
        producer.send("E2E message #" + i);
      }
      producer.flush();
      System.out.println(
          "Produced " + messageCount + " messages. Acked: " + producer.getUniqueAckCount());
    }

    // === Consume ===
    CursusConsumerConfig consConfig =
        CursusConsumerConfig.builder()
            .brokers(List.of("localhost:9000"))
            .topic(topic)
            .groupId("e2e-group")
            .consumerMode(ConsumerMode.POLLING)
            .maxPollRecords(100)
            .build();

    AtomicInteger received = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(messageCount);

    CursusConsumer consumer = new CursusConsumer(consConfig);

    Thread consumerThread =
        new Thread(
            () ->
                consumer.start(
                    message -> {
                      int count = received.incrementAndGet();
                      System.out.printf(
                          "  Consumed [%d]: offset=%d, payload=%s%n",
                          count, message.getOffset(), message.getPayload());
                      latch.countDown();
                    }));
    consumerThread.setDaemon(true);
    consumerThread.start();

    boolean success = latch.await(15, TimeUnit.SECONDS);
    consumer.close();

    System.out.println();
    if (success) {
      System.out.println("SUCCESS: Received all " + messageCount + " messages!");
    } else {
      System.out.println(
          "PARTIAL: Received "
              + received.get()
              + "/"
              + messageCount
              + " messages (timed out after 15s)");
    }
  }
}
