package io.cursus.client.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.cursus.client.admin.AdminClient.AdminConfig;
import io.cursus.client.admin.AdminClient.DeleteTopicOptions;
import io.cursus.client.admin.AdminClient.TopicCleanupPolicy;
import io.cursus.client.admin.AdminClient.TopicDefinitionPatch;
import io.cursus.client.admin.AdminClient.TruncateTopicOptions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class AdminClientTest {

  @Test
  void buildsCompletePatchAndParsesAuthoritativeDefinition() {
    ArrayDeque<String> responses =
        new ArrayDeque<>(
            List.of(
                "OK topic=orders revision=2 lifecycle_epoch=1 partitions=6 "
                    + "replication_factor=3 idempotent=true event_sourcing=false "
                    + "cleanup_policy=compact retention_hours=24 retention_bytes=4096 "
                    + "partitioner=hash_key auth_policy=acl read_acl=reader write_acl=writer"));
    List<String> calls = new ArrayList<>();
    AdminClient client =
        new AdminClient(
            (command, operation, retry) -> {
              calls.add(command + "|" + retry);
              return responses.remove();
            });

    var result =
        client.updateTopic(
            "orders",
            new TopicDefinitionPatch(
                6,
                3,
                true,
                false,
                TopicCleanupPolicy.COMPACT,
                24,
                4096L,
                "hash_key",
                "acl",
                List.of("reader"),
                List.of("writer")));

    assertThat(result.revision()).isEqualTo(2);
    assertThat(result.readAcl()).containsExactly("reader");
    assertThat(calls.get(0)).startsWith("CREATE topic=orders partitions=6").endsWith("|true");
  }

  @Test
  void destructiveRetriesRequireExplicitIdempotencyContract() {
    ArrayDeque<String> responses =
        new ArrayDeque<>(
            List.of(
                "OK topic=orders deleted=true cleanup_pending=false",
                "OK topic=orders truncated=true revision=3 lifecycle_epoch=2 leo=0 hwm=0"));
    List<Boolean> retry = new ArrayList<>();
    AdminClient client =
        new AdminClient(
            (command, operation, retryAmbiguous) -> {
              retry.add(retryAmbiguous);
              return responses.remove();
            });

    client.deleteTopic("orders", new DeleteTopicOptions(true));
    client.truncateTopic("orders", new TruncateTopicOptions(2));

    assertThat(retry).containsExactly(true, false);
  }

  @Test
  void rejectsCommandInjectionAndPartialCredentials() {
    assertThatThrownBy(
            () -> new AdminConfig(List.of("localhost:9000"), 3, 100, 5000, "none", "admin", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("together");
    AdminClient client = new AdminClient((command, operation, retry) -> "OK");
    assertThatThrownBy(
            () ->
                client.createTopic(
                    "orders injected=true",
                    new TopicDefinitionPatch(
                        null, null, null, null, null, null, null, null, null, null, null)))
        .hasMessageContaining("invalid topic");
  }
}
