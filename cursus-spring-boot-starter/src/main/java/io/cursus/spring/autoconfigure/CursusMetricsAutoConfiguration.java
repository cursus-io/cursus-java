package io.cursus.spring.autoconfigure;

import io.cursus.client.consumer.CursusConsumer;
import io.cursus.client.metrics.CursusConsumerMetrics;
import io.cursus.client.metrics.CursusProducerMetrics;
import io.cursus.client.producer.CursusProducer;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass({MeterRegistry.class, CursusProducer.class})
public class CursusMetricsAutoConfiguration {

  @Bean
  @ConditionalOnBean(CursusProducer.class)
  public CursusProducerMetrics cursusProducerMetrics(
      CursusProperties properties, MeterRegistry registry) {
    return new CursusProducerMetrics(registry, properties.getProducer().getTopic());
  }

  @Bean
  @ConditionalOnBean(CursusConsumer.class)
  public CursusConsumerMetrics cursusConsumerMetrics(
      CursusProperties properties, MeterRegistry registry) {
    return new CursusConsumerMetrics(
        registry, properties.getConsumer().getTopic(), properties.getConsumer().getGroupId());
  }
}
