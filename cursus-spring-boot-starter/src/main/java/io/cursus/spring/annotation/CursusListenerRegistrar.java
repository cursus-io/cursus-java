package io.cursus.spring.annotation;

import io.cursus.client.config.ConsumerMode;
import io.cursus.client.config.CursusConsumerConfig;
import io.cursus.client.consumer.CursusConsumer;
import io.cursus.client.util.ExecutorFactory;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class CursusListenerRegistrar
    implements BeanPostProcessor, DisposableBean, EnvironmentAware {

  private static final Logger log = LoggerFactory.getLogger(CursusListenerRegistrar.class);

  private final List<CursusConsumer> managedConsumers = new ArrayList<>();
  private final ExecutorService listenerExecutor =
      ExecutorFactory.create(Runtime.getRuntime().availableProcessors(), "cursus-listener");
  private Environment environment;

  @Override
  public void setEnvironment(Environment environment) {
    this.environment = environment;
  }

  @Override
  public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
    for (Method method : bean.getClass().getDeclaredMethods()) {
      CursusListener annotation = method.getAnnotation(CursusListener.class);
      if (annotation != null) {
        registerListener(bean, method, annotation);
      }
    }
    return bean;
  }

  private void registerListener(Object bean, Method method, CursusListener annotation) {
    List<String> brokers =
        Binder.get(environment)
            .bind("cursus.brokers", List.class)
            .orElse(List.of("localhost:9000"));

    CursusConsumerConfig config =
        CursusConsumerConfig.builder()
            .brokers(brokers)
            .topic(annotation.topic())
            .groupId(annotation.groupId())
            .consumerMode(
                "polling".equalsIgnoreCase(annotation.mode())
                    ? ConsumerMode.POLLING
                    : ConsumerMode.STREAMING)
            .build();

    CursusConsumer consumer = new CursusConsumer(config);
    managedConsumers.add(consumer);

    listenerExecutor.submit(
        () -> {
          log.info(
              "Starting @CursusListener: topic={}, group={}, method={}",
              annotation.topic(),
              annotation.groupId(),
              method.getName());
          consumer.start(
              message -> {
                try {
                  method.invoke(bean, message);
                } catch (Exception e) {
                  log.error(
                      "Error in @CursusListener handler {}: {}",
                      method.getName(),
                      e.getMessage(),
                      e);
                }
              });
        });
  }

  @Override
  public void destroy() {
    log.info("Shutting down {} managed @CursusListener consumers", managedConsumers.size());
    managedConsumers.forEach(CursusConsumer::close);
    listenerExecutor.shutdown();
  }
}
