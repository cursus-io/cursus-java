package io.cursus.examples.springboot;

import io.cursus.client.message.CursusMessage;
import io.cursus.spring.annotation.CursusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class EventListener {

  private static final Logger log = LoggerFactory.getLogger(EventListener.class);

  @CursusListener(topic = "example-topic", groupId = "spring-example-group")
  public void handleMessage(CursusMessage message) {
    log.info("Received message: offset={}, payload={}", message.getOffset(), message.getPayload());
  }
}
