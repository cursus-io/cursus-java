package io.cursus.client.saga;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cursus.client.producer.CursusProducer;
import io.cursus.client.saga.jdbc.JdbcHistoryOutboxPublisher;
import java.util.Map;

/** Bridges the service-owned history outbox to a configured Cursus observation topic. */
public final class CursusHistoryPublisher implements JdbcHistoryOutboxPublisher.Publisher {
  private static final ObjectMapper JSON = new ObjectMapper();

  private final CursusProducer producer;
  private final String topic;

  public CursusHistoryPublisher(CursusProducer producer, String topic) {
    if (producer == null || topic == null || topic.isBlank())
      throw new IllegalArgumentException("producer and history topic are required");
    this.producer = producer;
    this.topic = topic;
  }

  @Override
  public void publish(String topic, String payload) throws Exception {
    if (!this.topic.equals(topic))
      throw new IllegalArgumentException("history publisher topic does not match configured topic");
    Map<String, Object> event = JSON.readValue(payload, new TypeReference<>() {});
    producer.send(payload, String.valueOf(event.get("history_event_id")));
    producer.flush();
  }
}
