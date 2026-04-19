package io.cursus.spring.autoconfigure;

import io.cursus.client.consumer.CursusConsumer;
import io.cursus.client.producer.CursusProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnClass(HealthIndicator.class)
public class CursusHealthIndicator implements HealthIndicator {

  private final CursusProperties properties;

  @Autowired(required = false)
  private CursusProducer producer;

  @Autowired(required = false)
  private CursusConsumer consumer;

  public CursusHealthIndicator(CursusProperties properties) {
    this.properties = properties;
  }

  @Override
  public Health health() {
    boolean producerUp = producer != null && producer.isConnected();
    boolean consumerUp = consumer != null && consumer.isConnected();

    Health.Builder builder = (producerUp || consumerUp) ? Health.up() : Health.down();

    builder.withDetail("brokers", properties.getBrokers());
    if (producer != null) builder.withDetail("producer", producerUp ? "connected" : "disconnected");
    if (consumer != null) builder.withDetail("consumer", consumerUp ? "connected" : "disconnected");

    return builder.build();
  }
}
