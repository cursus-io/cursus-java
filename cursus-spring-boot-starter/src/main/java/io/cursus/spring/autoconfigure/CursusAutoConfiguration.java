package io.cursus.spring.autoconfigure;

import io.cursus.client.config.Acks;
import io.cursus.client.config.ConsumerMode;
import io.cursus.client.config.CursusConsumerConfig;
import io.cursus.client.config.CursusProducerConfig;
import io.cursus.client.consumer.CursusConsumer;
import io.cursus.client.producer.CursusProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(CursusProducer.class)
@EnableConfigurationProperties(CursusProperties.class)
public class CursusAutoConfiguration {

  @Autowired(required = false)
  private Object meterRegistry;

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnProperty(prefix = "cursus.producer", name = "topic")
  public CursusProducer cursusProducer(CursusProperties props) {
    CursusProperties.Producer p = props.getProducer();
    CursusProducerConfig config =
        CursusProducerConfig.builder()
            .brokers(props.getBrokers())
            .topic(p.getTopic())
            .partitions(p.getPartitions())
            .acks(parseAcks(p.getAcks()))
            .batchSize(p.getBatchSize())
            .lingerMs(p.getLingerMs())
            .idempotent(p.isIdempotent())
            .compressionType(p.getCompressionType())
            .maxInflightRequests(p.getMaxInflightRequests())
            .flushTimeoutMs(p.getFlushTimeoutMs())
            .tlsCertPath(p.getTlsCertPath())
            .tlsKeyPath(p.getTlsKeyPath())
            .build();
    return new CursusProducer(config, meterRegistry);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnProperty(prefix = "cursus.consumer", name = "topic")
  public CursusConsumer cursusConsumer(CursusProperties props) {
    CursusProperties.Consumer c = props.getConsumer();
    CursusConsumerConfig config =
        CursusConsumerConfig.builder()
            .brokers(props.getBrokers())
            .topic(c.getTopic())
            .groupId(c.getGroupId())
            .consumerMode(
                "polling".equalsIgnoreCase(c.getMode())
                    ? ConsumerMode.POLLING
                    : ConsumerMode.STREAMING)
            .autoCommitInterval(c.getAutoCommitInterval())
            .maxPollRecords(c.getMaxPollRecords())
            .sessionTimeoutMs(c.getSessionTimeoutMs())
            .heartbeatIntervalMs(c.getHeartbeatIntervalMs())
            .tlsCertPath(c.getTlsCertPath())
            .tlsKeyPath(c.getTlsKeyPath())
            .build();
    return new CursusConsumer(config, meterRegistry);
  }

  private Acks parseAcks(String value) {
    return switch (value.toLowerCase()) {
      case "none", "0" -> Acks.NONE;
      case "all", "-1" -> Acks.ALL;
      default -> Acks.ONE;
    };
  }
}
