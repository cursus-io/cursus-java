package io.cursus.client.protocol;

/** Builds text-based Cursus protocol commands using key=value format. */
public final class CommandBuilder {

  private CommandBuilder() {}

  public static String create(String topic, int partitions) {
    return "CREATE topic=" + topic + " partitions=" + partitions;
  }

  public static String delete(String topic) {
    return "DELETE topic=" + topic;
  }

  public static String list() {
    return "LIST";
  }

  public static String list(String topic) {
    return "LIST topic=" + topic;
  }

  public static String subscribe(String topic, String group) {
    return "SUBSCRIBE topic=" + topic + " group=" + group;
  }

  public static String joinGroup(String topic, String group, String member) {
    return "JOIN_GROUP topic=" + topic + " group=" + group + " member=" + member;
  }

  public static String syncGroup(String topic, String group, String member, int generation) {
    return "SYNC_GROUP topic="
        + topic
        + " group="
        + group
        + " member="
        + member
        + " generation="
        + generation;
  }

  public static String heartbeat(String topic, String group, String member, int generation) {
    return "HEARTBEAT topic="
        + topic
        + " group="
        + group
        + " member="
        + member
        + " generation="
        + generation;
  }

  public static String consume(
      String topic, int partition, long offset, String group, int generation, String member) {
    return consume(topic, partition, offset, group, generation, member, null, null, null);
  }

  public static String consume(
      String topic,
      int partition,
      long offset,
      String group,
      int generation,
      String member,
      String isolationLevel,
      String principal,
      String authToken) {
    String command =
        "CONSUME topic="
            + topic
            + " partition="
            + partition
            + " offset="
            + offset
            + " group="
            + group
            + " generation="
            + generation
            + " member="
            + member;
    if (isolationLevel != null && !isolationLevel.isBlank()) {
      command += " isolation_level=" + isolationLevel;
    }
    return appendAuth(command, principal, authToken);
  }

  public static String stream(
      String topic, int partition, String group, long offset, int generation, String member) {
    return stream(topic, partition, group, offset, generation, member, null, null, null);
  }

  public static String stream(
      String topic,
      int partition,
      String group,
      long offset,
      int generation,
      String member,
      String isolationLevel,
      String principal,
      String authToken) {
    String command =
        "STREAM topic="
            + topic
            + " partition="
            + partition
            + " group="
            + group
            + " offset="
            + offset
            + " generation="
            + generation
            + " member="
            + member;
    if (isolationLevel != null && !isolationLevel.isBlank()) {
      command += " isolation_level=" + isolationLevel;
    }
    return appendAuth(command, principal, authToken);
  }

  public static String batchCommit(
      String topic, String group, int generation, String member, String offsets) {
    return "BATCH_COMMIT topic="
        + topic
        + " group="
        + group
        + " member="
        + member
        + " generation="
        + generation
        + " "
        + offsets;
  }

  public static String commitOffset(
      String topic, int partition, String group, long offset, int generation, String member) {
    return "COMMIT_OFFSET topic="
        + topic
        + " partition="
        + partition
        + " group="
        + group
        + " offset="
        + offset
        + " generation="
        + generation
        + " member="
        + member;
  }

  public static String fetchOffset(String topic, int partition, String group) {
    return "FETCH_OFFSET topic=" + topic + " partition=" + partition + " group=" + group;
  }

  public static String leaveGroup(String topic, String group, String member) {
    return "LEAVE_GROUP topic=" + topic + " group=" + group + " member=" + member;
  }

  public static String findCoordinator(String group) {
    return "FIND_COORDINATOR group=" + group;
  }

  public static String findTransactionCoordinator(String transactionalId) {
    return "FIND_COORDINATOR transactional_id=" + transactionalId;
  }

  public static String listOffsets(String topic) {
    return listOffsets(topic, null, null, null);
  }

  public static String listOffsets(
      String topic, Integer partition, String principal, String authToken) {
    String command = "LIST_OFFSETS topic=" + topic;
    if (partition != null) {
      command += " partition=" + partition;
    }
    return appendAuth(command, principal, authToken);
  }

  public static String initProducerId(String transactionalId) {
    return "INIT_PRODUCER_ID transactional_id=" + transactionalId;
  }

  public static String beginTxn(String transactionalId, String producerId, long epoch) {
    return "BEGIN_TXN transactional_id="
        + transactionalId
        + " producerId="
        + producerId
        + " epoch="
        + epoch;
  }

  public static String txnPublish(
      String transactionalId,
      String topic,
      int partition,
      String producerId,
      long seqNum,
      long epoch,
      String message,
      String key,
      String principal,
      String authToken) {
    String command =
        "TXN_PUBLISH transactional_id="
            + transactionalId
            + " topic="
            + topic
            + " partition="
            + partition
            + " producerId="
            + producerId
            + " seqNum="
            + seqNum
            + " epoch="
            + epoch;
    if (key != null && !key.isBlank()) {
      command += " key=" + key;
    }
    command += " message=" + message;
    return appendAuth(command, principal, authToken);
  }

  public static String sendOffsetsToTxn(
      String transactionalId,
      String producerId,
      long epoch,
      String topic,
      String group,
      String member,
      int generation,
      String offsets) {
    return "SEND_OFFSETS_TO_TXN transactional_id="
        + transactionalId
        + " producerId="
        + producerId
        + " epoch="
        + epoch
        + " topic="
        + topic
        + " group="
        + group
        + " member="
        + member
        + " generation="
        + generation
        + " "
        + offsets;
  }

  public static String endTxn(
      String transactionalId, String producerId, long epoch, boolean commit) {
    return "END_TXN transactional_id="
        + transactionalId
        + " producerId="
        + producerId
        + " epoch="
        + epoch
        + " result="
        + (commit ? "commit" : "abort");
  }

  public static String txnStatus(String transactionalId) {
    return "TXN_STATUS transactional_id=" + transactionalId;
  }

  public static String metadata(String topic) {
    return "METADATA topic=" + topic;
  }

  private static String appendAuth(String command, String principal, String authToken) {
    if (principal != null && !principal.isBlank()) {
      command += " principal=" + principal;
    }
    if (authToken != null && !authToken.isBlank()) {
      command += " auth_token=" + authToken;
    }
    return command;
  }
}
