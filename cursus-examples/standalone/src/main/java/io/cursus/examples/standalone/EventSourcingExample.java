package io.cursus.examples.standalone;

import io.cursus.client.eventstore.*;

public class EventSourcingExample {

  public static void main(String[] args) {
    String broker = args.length > 0 ? args[0] : "localhost:9000";

    try (CursusEventStore es = new CursusEventStore(broker, "orders-es", "order-svc")) {
      es.createTopic(4);

      String key = "order-" + System.currentTimeMillis();

      AppendResult r1 =
          es.append(
              key,
              1,
              Event.builder()
                  .type("OrderCreated")
                  .payload("{\"item\":\"widget\",\"qty\":5}")
                  .build());
      System.out.printf("Append 1: version=%d offset=%d%n", r1.getVersion(), r1.getOffset());

      AppendResult r2 =
          es.append(
              key,
              2,
              Event.builder().type("OrderShipped").payload("{\"tracking\":\"ABC123\"}").build());
      System.out.printf("Append 2: version=%d offset=%d%n", r2.getVersion(), r2.getOffset());

      long ver = es.streamVersion(key);
      System.out.printf("StreamVersion: %d%n", ver);

      StreamData stream = es.readStream(key);
      System.out.printf("%nStream for %s (%d events):%n", key, stream.getEvents().size());
      for (StreamEvent evt : stream.getEvents()) {
        System.out.printf("  v%d: %s -- %s%n", evt.getVersion(), evt.getType(), evt.getPayload());
      }

      es.saveSnapshot(key, ver, "{\"state\":\"shipped\"}");
      Snapshot snap = es.readSnapshot(key);
      if (snap != null) {
        System.out.printf("Snapshot: v%d payload=%s%n", snap.getVersion(), snap.getPayload());
      }

      // Version conflict test
      try {
        es.append(key, 1, Event.builder().type("Stale").payload("{}").build());
      } catch (Exception e) {
        System.out.printf("Version conflict (expected): %s%n", e.getMessage());
      }
    }
  }
}
