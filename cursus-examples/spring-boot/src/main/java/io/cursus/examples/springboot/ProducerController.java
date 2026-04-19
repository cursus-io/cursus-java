package io.cursus.examples.springboot;

import io.cursus.client.producer.CursusProducer;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/messages")
public class ProducerController {

  private final CursusProducer producer;

  public ProducerController(CursusProducer producer) {
    this.producer = producer;
  }

  @PostMapping
  public SendResponse send(
      @RequestBody String payload, @RequestParam(required = false) String key) {
    long seq = (key != null) ? producer.send(payload, key) : producer.send(payload);
    return new SendResponse(seq, producer.getUniqueAckCount());
  }

  @PostMapping("/flush")
  public String flush() {
    producer.flush();
    return "Flushed. Total acked: " + producer.getUniqueAckCount();
  }

  record SendResponse(long seqNum, long totalAcked) {}
}
