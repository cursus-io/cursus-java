package io.cursus.client.framework;

import io.cursus.client.eventstore.AppendResult;
import io.cursus.client.eventstore.StreamData;
import io.cursus.client.eventstore.StreamEvent;
import java.util.List;
import java.util.function.Function;

/** Optimistic-concurrency repository for event-sourced aggregates. */
public final class AggregateRepository<A extends Aggregate> {
  private final StreamStore store;
  private final Function<String, A> factory;

  public AggregateRepository(StreamStore store, Function<String, A> factory) {
    if (store == null || factory == null) {
      throw new IllegalArgumentException("stream store and aggregate factory are required");
    }
    this.store = store;
    this.factory = factory;
  }

  public A load(String id) {
    A aggregate = factory.apply(id);
    if (aggregate == null) {
      throw new IllegalArgumentException("aggregate factory returned null for " + id);
    }
    StreamData stream = store.readStream(id);
    if (stream.getSnapshot() != null) {
      if (!(aggregate instanceof SnapshotRestorer restorer)) {
        throw new IllegalArgumentException(
            "aggregate has a snapshot but does not implement SnapshotRestorer");
      }
      restorer.restoreSnapshot(
          stream.getSnapshot().getPayload(), stream.getSnapshot().getVersion());
    }
    for (StreamEvent raw : stream.getEvents()) {
      EventEnvelope event = EventEnvelope.fromStreamEvent(raw);
      if (!id.equals(event.aggregateId())) {
        throw new IllegalArgumentException("event aggregate id does not match requested aggregate");
      }
      try {
        aggregate.apply(event);
      } catch (RuntimeException exception) {
        throw new IllegalStateException(
            "apply " + event.eventType() + " v" + event.aggregateVersion(), exception);
      }
    }
    return aggregate;
  }

  public void save(A aggregate, List<EventEnvelope> events) {
    if (aggregate == null) throw new IllegalArgumentException("aggregate is required");
    if (events.size() > 1) {
      throw new IllegalArgumentException(
          "saving multiple events is not supported without atomic batch append");
    }
    long expected = aggregate.version();
    for (EventEnvelope source : events) {
      long next = ++expected;
      String aggregateType =
          blank(source.aggregateType()) ? aggregate.type() : source.aggregateType();
      String aggregateId = blank(source.aggregateId()) ? aggregate.id() : source.aggregateId();
      if (!aggregate.id().equals(aggregateId)) {
        throw new IllegalArgumentException("event aggregate id does not match aggregate");
      }
      EventEnvelope event =
          new EventEnvelope(
              source.eventId(),
              source.eventType(),
              source.schemaVersion(),
              aggregateType,
              aggregateId,
              next,
              source.occurredAt(),
              source.correlationId(),
              source.associationKey(),
              source.causationId(),
              source.payload());
      AppendResult result = store.append(aggregate.id(), next - 1, event.toEvent());
      if (result.getVersion() != next) {
        throw new IllegalStateException(
            "append returned version " + result.getVersion() + ", expected " + next);
      }
      aggregate.apply(event);
    }
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
