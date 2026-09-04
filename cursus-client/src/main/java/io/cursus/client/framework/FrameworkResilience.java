package io.cursus.client.framework;

import io.cursus.client.eventstore.StreamEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

public final class FrameworkResilience {
  private FrameworkResilience() {}

  public record RetryPolicy(
      int maxAttempts, Duration initialDelay, Duration maxDelay, double multiplier) {
    public boolean shouldRetry(int attempt) {
      return maxAttempts > 0 && attempt < maxAttempts;
    }

    public Duration delay(int attempt) {
      attempt = Math.max(1, attempt);
      Duration initial =
          initialDelay == null || initialDelay.isNegative() || initialDelay.isZero()
              ? Duration.ofSeconds(1)
              : initialDelay;
      double factor = multiplier < 1 ? 2 : multiplier;
      long millis = Math.round(initial.toMillis() * Math.pow(factor, attempt - 1));
      Duration result = Duration.ofMillis(millis);
      return maxDelay != null && !maxDelay.isZero() && result.compareTo(maxDelay) > 0
          ? maxDelay
          : result;
    }
  }

  public static final class UpcasterRegistry {
    private final Map<String, Map<Integer, UnaryOperator<EventEnvelope>>> entries = new HashMap<>();

    public synchronized void register(
        String eventType, int fromVersion, UnaryOperator<EventEnvelope> upcaster) {
      if (eventType == null || eventType.isBlank() || fromVersion <= 0 || upcaster == null) {
        throw new IllegalArgumentException("event type, source version, and upcaster are required");
      }
      Map<Integer, UnaryOperator<EventEnvelope>> versions =
          entries.computeIfAbsent(eventType, ignored -> new HashMap<>());
      if (versions.putIfAbsent(fromVersion, upcaster) != null) {
        throw new IllegalArgumentException(
            "upcaster already registered for " + eventType + " v" + fromVersion);
      }
    }

    public EventEnvelope upcast(EventEnvelope source) {
      EventEnvelope event = source;
      while (true) {
        UnaryOperator<EventEnvelope> upcaster;
        synchronized (this) {
          upcaster = entries.getOrDefault(event.eventType(), Map.of()).get(event.schemaVersion());
        }
        if (upcaster == null) return event;
        EventEnvelope updated = upcaster.apply(event);
        if (updated.schemaVersion() <= event.schemaVersion()) {
          throw new IllegalArgumentException(
              "upcaster for " + event.eventType() + " did not advance schema version");
        }
        event = updated;
      }
    }
  }

  public static void replay(
      StreamStore store,
      String key,
      long fromVersion,
      UpcasterRegistry registry,
      Consumer<EventEnvelope> handler) {
    if (store == null || handler == null) {
      throw new IllegalArgumentException("stream store and replay handler are required");
    }
    for (StreamEvent raw : store.readStream(key).getEvents()) {
      EventEnvelope event = EventEnvelope.fromStreamEvent(raw);
      if (fromVersion > 0 && event.aggregateVersion() < fromVersion) continue;
      if (registry != null) event = registry.upcast(event);
      try {
        handler.accept(event);
      } catch (RuntimeException exception) {
        throw new IllegalStateException(
            "replay " + event.eventType() + " v" + event.aggregateVersion(), exception);
      }
    }
  }

  public static final class DeadlineManager {
    private record Deadline(Instant at, Runnable callback) {}

    private final Map<String, Deadline> entries = new HashMap<>();

    public synchronized void schedule(String id, Instant at, Runnable callback) {
      if (id == null || id.isBlank() || at == null || callback == null) {
        throw new IllegalArgumentException("deadline id, time, and callback are required");
      }
      entries.put(id, new Deadline(at, callback));
    }

    public synchronized void cancel(String id) {
      entries.remove(id);
    }

    public int runDue(Instant now) {
      List<Runnable> due = new ArrayList<>();
      synchronized (this) {
        entries
            .entrySet()
            .removeIf(
                entry -> {
                  if (!entry.getValue().at().isAfter(now)) {
                    due.add(entry.getValue().callback());
                    return true;
                  }
                  return false;
                });
      }
      due.forEach(Runnable::run);
      return due.size();
    }
  }
}
