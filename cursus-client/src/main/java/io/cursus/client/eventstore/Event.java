package io.cursus.client.eventstore;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class Event {
  private final String type;
  private final String payload;
  @Builder.Default private final int schemaVersion = 1;
  @Builder.Default private final String metadata = "";
}
