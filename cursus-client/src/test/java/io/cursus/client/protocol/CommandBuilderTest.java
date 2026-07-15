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
    assertThat(CommandBuilder.batchCommit("t", "g", 1, "m1", "P0:100,P1:200"))
        .isEqualTo("BATCH_COMMIT topic=t group=g member=m1 generation=1 P0:100,P1:200");
  }

  @Test
  void subscribe() {
    assertThat(CommandBuilder.subscribe("t", "g")).isEqualTo("SUBSCRIBE topic=t group=g");
  }

  @Test
  void consumeWithIsolationAndAuth() {
    assertThat(
            CommandBuilder.consume("t", 0, 10, "g", 2, "m1", "read_committed", "alice", "secret"))
        .isEqualTo(
            "CONSUME topic=t partition=0 offset=10 group=g generation=2 member=m1 "
                + "isolation_level=read_committed principal=alice auth_token=secret");
  }

  @Test
  void listOffsetsAndTransactionCommands() {
    assertThat(CommandBuilder.listOffsets("orders", 1, "alice", "secret"))
        .isEqualTo("LIST_OFFSETS topic=orders partition=1 principal=alice auth_token=secret");
    assertThat(CommandBuilder.findTransactionCoordinator("tx-1"))
        .isEqualTo("FIND_COORDINATOR transactional_id=tx-1");
    assertThat(CommandBuilder.initProducerId("tx-1"))
        .isEqualTo("INIT_PRODUCER_ID transactional_id=tx-1");
    assertThat(CommandBuilder.beginTxn("tx-1", "p1", 2))
        .isEqualTo("BEGIN_TXN transactional_id=tx-1 producerId=p1 epoch=2");
    assertThat(
            CommandBuilder.txnPublish(
                "tx-1", "out", -1, "p1", 3, 2, "processed", "k1", "alice", "secret"))
        .isEqualTo(
            "TXN_PUBLISH transactional_id=tx-1 topic=out partition=-1 producerId=p1 "
                + "seqNum=3 epoch=2 key=k1 message=processed principal=alice auth_token=secret");
    assertThat(
            CommandBuilder.sendOffsetsToTxn(
                "tx-1", "p1", 2, "input", "grp", "m1", 4, "P0:101,P2:202"))
        .isEqualTo(
            "SEND_OFFSETS_TO_TXN transactional_id=tx-1 producerId=p1 epoch=2 topic=input "
                + "group=grp member=m1 generation=4 P0:101,P2:202");
    assertThat(CommandBuilder.endTxn("tx-1", "p1", 2, false))
        .isEqualTo("END_TXN transactional_id=tx-1 producerId=p1 epoch=2 result=abort");
    assertThat(CommandBuilder.txnStatus("tx-1")).isEqualTo("TXN_STATUS transactional_id=tx-1");
  }
}
