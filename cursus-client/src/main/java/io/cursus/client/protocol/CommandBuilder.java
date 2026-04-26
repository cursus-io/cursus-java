package io.cursus.client.protocol;

/**
 * Builds text-based Cursus protocol commands using key=value format. These commands are sent as
 * UTF-8 strings over the length-prefixed TCP connection. Matches the Go SDK's wire protocol.
 */
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
    return "CONSUME topic="
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
  }

  public static String stream(
      String topic, int partition, String group, long offset, int generation, String member) {
    return "STREAM topic="
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

  public static String batchCommit(
      String topic, String group, int generation, String member, String offsets) {
    return "BATCH_COMMIT topic="
        + topic
        + " group="
        + group
        + " generation="
        + generation
        + " member="
        + member
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

  public static String metadata(String topic) {
    return "METADATA topic=" + topic;
  }
}
