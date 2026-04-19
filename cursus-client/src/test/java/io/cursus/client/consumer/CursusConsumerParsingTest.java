package io.cursus.client.consumer;

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Tests for the regex-based parsing of JOIN_GROUP and SYNC_GROUP responses as used in
 * CursusConsumer. The patterns are replicated here so they can be tested independently of a broker
 * connection.
 */
class CursusConsumerParsingTest {

  // Replicate the patterns from CursusConsumer so they can be tested directly.
  private static final Pattern JOIN_RESPONSE_PATTERN =
      Pattern.compile(
          "OK\\s+generation=(\\d+)\\s+member=(\\S+)(?:\\s+assignments=\\[([\\d\\s]*)\\])?");
  private static final Pattern PARTITION_LIST_PATTERN = Pattern.compile("\\[([\\d\\s]*)\\]");

  // -------------------------------------------------------------------------
  // JOIN_RESPONSE_PATTERN tests
  // -------------------------------------------------------------------------

  @Test
  void parseJoinResponseWithInlineAssignments() {
    String response = "OK generation=3 member=abc-123 assignments=[0 1 2]";

    Matcher m = JOIN_RESPONSE_PATTERN.matcher(response);
    assertThat(m.matches()).isTrue();
    assertThat(m.group(1)).isEqualTo("3");
    assertThat(m.group(2)).isEqualTo("abc-123");
    assertThat(m.group(3)).isEqualTo("0 1 2");
  }

  @Test
  void parseJoinResponseWithoutAssignments() {
    String response = "OK generation=1 member=member-xyz";

    Matcher m = JOIN_RESPONSE_PATTERN.matcher(response);
    assertThat(m.matches()).isTrue();
    assertThat(m.group(1)).isEqualTo("1");
    assertThat(m.group(2)).isEqualTo("member-xyz");
    assertThat(m.group(3)).isNull();
  }

  @Test
  void parseJoinResponseWithEmptyAssignments() {
    String response = "OK generation=2 member=m1 assignments=[]";

    Matcher m = JOIN_RESPONSE_PATTERN.matcher(response);
    assertThat(m.matches()).isTrue();
    assertThat(m.group(1)).isEqualTo("2");
    assertThat(m.group(2)).isEqualTo("m1");
    assertThat(m.group(3)).isEqualTo("");
  }

  @Test
  void parseJoinResponseInvalidFormatReturnsNoMatch() {
    String response = "ERR unknown command";

    Matcher m = JOIN_RESPONSE_PATTERN.matcher(response);
    assertThat(m.matches()).isFalse();
  }

  // -------------------------------------------------------------------------
  // PARTITION_LIST_PATTERN tests (SYNC_GROUP response)
  // -------------------------------------------------------------------------

  @Test
  void parseSyncGroupPartitions() {
    String response = "OK [0 1 2 3]";

    Matcher m = PARTITION_LIST_PATTERN.matcher(response);
    assertThat(m.find()).isTrue();
    List<Integer> partitions = parsePartitionList(m.group(1));
    assertThat(partitions).containsExactly(0, 1, 2, 3);
  }

  @Test
  void parseEmptyPartitionList() {
    String response = "OK []";

    Matcher m = PARTITION_LIST_PATTERN.matcher(response);
    assertThat(m.find()).isTrue();
    List<Integer> partitions = parsePartitionList(m.group(1));
    assertThat(partitions).isEmpty();
  }

  // -------------------------------------------------------------------------
  // parsePartitionList helper tests
  // -------------------------------------------------------------------------

  @Test
  void parsePartitionListFromString() {
    List<Integer> result = parsePartitionList("0 1 2");
    assertThat(result).containsExactly(0, 1, 2);
  }

  @Test
  void parsePartitionListFromNullReturnsEmpty() {
    assertThat(parsePartitionList(null)).isEmpty();
  }

  @Test
  void parsePartitionListFromBlankReturnsEmpty() {
    assertThat(parsePartitionList("")).isEmpty();
    assertThat(parsePartitionList("   ")).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Helper (mirrors CursusConsumer#parsePartitionList)
  // -------------------------------------------------------------------------

  private List<Integer> parsePartitionList(String listStr) {
    if (listStr == null || listStr.isBlank()) return List.of();
    List<Integer> result = new java.util.ArrayList<>();
    for (String token : listStr.trim().split("\\s+")) {
      if (!token.isEmpty()) {
        result.add(Integer.parseInt(token));
      }
    }
    return result;
  }
}
