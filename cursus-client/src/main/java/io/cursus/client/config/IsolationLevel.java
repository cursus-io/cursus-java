package io.cursus.client.config;

public enum IsolationLevel {
  READ_UNCOMMITTED("read_uncommitted"),
  READ_COMMITTED("read_committed");

  private final String wireValue;

  IsolationLevel(String wireValue) {
    this.wireValue = wireValue;
  }

  public String wireValue() {
    return wireValue;
  }
}
