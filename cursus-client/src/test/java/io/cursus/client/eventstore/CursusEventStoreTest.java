package io.cursus.client.eventstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

class CursusEventStoreTest {

  @Test
  void parseAppendResponseStrictContract() throws Exception {
    CursusEventStore store = new CursusEventStore("localhost:9000", "orders", "producer-1");

    AppendResult result = parseAppendResponse(store, "OK version=3 offset=42 partition=1");

    assertThat(result.getVersion()).isEqualTo(3);
    assertThat(result.getOffset()).isEqualTo(42);
    assertThat(result.getPartition()).isEqualTo(1);
  }

  @Test
  void parseAppendResponseRejectsNonContractResponses() throws Exception {
    CursusEventStore store = new CursusEventStore("localhost:9000", "orders", "producer-1");

    assertThatThrownBy(() -> parseAppendResponse(store, "OK version=3"))
        .isInstanceOf(InvocationTargetException.class)
        .hasRootCauseMessage("append: missing fields in response: OK version=3");
    assertThatThrownBy(() -> parseAppendResponse(store, "3"))
        .isInstanceOf(InvocationTargetException.class)
        .hasRootCauseMessage("append: unexpected response: 3");
  }

  private static AppendResult parseAppendResponse(CursusEventStore store, String response)
      throws Exception {
    Method method = CursusEventStore.class.getDeclaredMethod("parseAppendResponse", String.class);
    method.setAccessible(true);
    return (AppendResult) method.invoke(store, response);
  }
}
