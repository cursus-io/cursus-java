package io.cursus.client.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CommandBuilderTest {

  @Test
  void createTopic() {
    assertThat(CommandBuilder.create("my-topic", 4))
        .isEqualTo("CREATE topic=my-topic partitions=4");
  }

  @Test
  void deleteTopic() {
    assertThat(CommandBuilder.delete("my-topic")).isEqualTo("DELETE topic=my-topic");
  }

  @Test
  void listTopics() {
    assertThat(CommandBuilder.list()).isEqualTo("LIST");
  }

  @Test
  void listTopicsWithFilter() {
    assertThat(CommandBuilder.list("my-topic")).isEqualTo("LIST topic=my-topic");
  }

  @Test
  void consume() {
    assertThat(CommandBuilder.consume("t", 2, 100, "g", 1, "m1"))
        .isEqualTo("CONSUME topic=t partition=2 offset=100 group=g generation=1 member=m1");
  }

  @Test
  void joinGroup() {
    assertThat(CommandBuilder.joinGroup("t", "g", "c1"))
        .isEqualTo("JOIN_GROUP topic=t group=g member=c1");
  }

  @Test
  void syncGroup() {
    assertThat(CommandBuilder.syncGroup("t", "g", "m1", 3))
        .isEqualTo("SYNC_GROUP topic=t group=g member=m1 generation=3");
  }

  @Test
  void heartbeat() {
    assertThat(CommandBuilder.heartbeat("t", "g", "m1", 2))
        .isEqualTo("HEARTBEAT topic=t group=g member=m1 generation=2");
  }

  @Test
  void leaveGroup() {
    assertThat(CommandBuilder.leaveGroup("t", "g", "c1"))
        .isEqualTo("LEAVE_GROUP topic=t group=g member=c1");
  }

  @Test
  void stream() {
    assertThat(CommandBuilder.stream("t", 0, "g", 50, 1, "m1"))
        .isEqualTo("STREAM topic=t partition=0 group=g offset=50 generation=1 member=m1");
  }

  @Test
  void commitOffset() {
    assertThat(CommandBuilder.commitOffset("t", 1, "g", 200, 2, "m1"))
        .isEqualTo("COMMIT_OFFSET topic=t partition=1 group=g offset=200 generation=2 member=m1");
  }

  @Test
  void fetchOffset() {
    assertThat(CommandBuilder.fetchOffset("t", 1, "g"))
        .isEqualTo("FETCH_OFFSET topic=t partition=1 group=g");
  }

  @Test
  void batchCommit() {
    assertThat(CommandBuilder.batchCommit("t", "g", 1, "m1", "0:100,1:200"))
        .isEqualTo("BATCH_COMMIT topic=t group=g generation=1 member=m1 0:100,1:200");
  }

  @Test
  void subscribe() {
    assertThat(CommandBuilder.subscribe("t", "g")).isEqualTo("SUBSCRIBE topic=t group=g");
  }
}
