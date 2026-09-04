package io.cursus.client.framework;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.cursus.client.eventstore.AppendResult;
import io.cursus.client.eventstore.Event;
import io.cursus.client.eventstore.StreamData;
import io.cursus.client.eventstore.StreamEvent;
import io.cursus.client.framework.FrameworkResilience.DeadlineManager;
import io.cursus.client.framework.FrameworkResilience.RetryPolicy;
import io.cursus.client.framework.FrameworkResilience.UpcasterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class EventFrameworkTest {
  private static final class MemoryStore implements StreamStore {
    private final List<StreamEvent> events = new ArrayList<>();

    @Override
    public StreamData readStream(String key) {
      return new StreamData(null, List.copyOf(events));
    }

    @Override
    public AppendResult append(String key, long expectedVersion, Event event) {
      long version = expectedVersion + 1;
      events.add(
          new StreamEvent(
              version,
              version - 1,
              event.getType(),
              event.getSchemaVersion(),
              event.getPayload(),
              event.getMetadata()));
      return new AppendResult(version, version - 1, 0);
    }
  }

  private static final class Game implements Aggregate {
    private final String id;
    private long version;
    private String status = "";

    private Game(String id) {
      this.id = id;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public String type() {
      return "game";
    }

    @Override
    public long version() {
      return version;
    }

    @Override
    public void apply(EventEnvelope event) {
      version = event.aggregateVersion();
      status = event.payload().path("status").asText();
    }
  }

  @Test
  void envelopeRepositoryAndReplayMatchGoContract() {
    MemoryStore store = new MemoryStore();
    AggregateRepository<Game> repository = new AggregateRepository<>(store, Game::new);
    Game aggregate = new Game("game-1");
    EventEnvelope event =
        EventEnvelope.create("game", "game-1", "GameCreated", java.util.Map.of("status", "open"));

    repository.save(aggregate, List.of(event));
    Game loaded = repository.load("game-1");

    assertThat(aggregate.version).isEqualTo(1);
    assertThat(loaded.status).isEqualTo("open");
    List<Long> replayed = new ArrayList<>();
    FrameworkResilience.replay(
        store, "game-1", 1, null, value -> replayed.add(value.aggregateVersion()));
    assertThat(replayed).containsExactly(1L);
  }

  @Test
  void rejectsNonAtomicMultiEventSave() {
    MemoryStore store = new MemoryStore();
    AggregateRepository<Game> repository = new AggregateRepository<>(store, Game::new);
    assertThatThrownBy(
            () ->
                repository.save(
                    new Game("game-1"),
                    List.of(
                        EventEnvelope.create("game", "game-1", "A", java.util.Map.of()),
                        EventEnvelope.create("game", "game-1", "B", java.util.Map.of()))))
        .hasMessageContaining("atomic batch");
    assertThat(store.events).isEmpty();
  }

  @Test
  void upcastingRetryAndDeadlinesAreBoundedAndDeterministic() {
    EventEnvelope event =
        EventEnvelope.create("game", "game-1", "Updated", java.util.Map.of("v", 1))
            .withAggregateVersion(1);
    UpcasterRegistry registry = new UpcasterRegistry();
    registry.register(
        "Updated",
        1,
        value -> value.withSchemaVersion(2, JsonNodeFactory.instance.objectNode().put("v", 2)));
    assertThat(registry.upcast(event).schemaVersion()).isEqualTo(2);

    RetryPolicy policy = new RetryPolicy(3, Duration.ofMillis(10), Duration.ofMillis(25), 2);
    assertThat(policy.delay(1)).isEqualTo(Duration.ofMillis(10));
    assertThat(policy.delay(2)).isEqualTo(Duration.ofMillis(20));
    assertThat(policy.delay(3)).isEqualTo(Duration.ofMillis(25));

    DeadlineManager manager = new DeadlineManager();
    Instant now = Instant.ofEpochSecond(100);
    List<String> fired = new ArrayList<>();
    manager.schedule("one", now, () -> fired.add("one"));
    assertThat(manager.runDue(now)).isEqualTo(1);
    assertThat(manager.runDue(now)).isZero();
    assertThat(fired).containsExactly("one");
  }
}
