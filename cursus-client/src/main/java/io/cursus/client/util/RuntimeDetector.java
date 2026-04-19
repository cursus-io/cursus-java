package io.cursus.client.util;

/**
 * Detects Java runtime capabilities for adaptive behavior. Used to auto-enable Virtual Threads on
 * Java 21+.
 */
public final class RuntimeDetector {

  private static final int JAVA_VERSION;
  private static final boolean VIRTUAL_THREADS_SUPPORTED;

  static {
    JAVA_VERSION = parseJavaVersion();
    VIRTUAL_THREADS_SUPPORTED = checkVirtualThreads();
  }

  private RuntimeDetector() {}

  public static int javaVersion() {
    return JAVA_VERSION;
  }

  public static boolean supportsVirtualThreads() {
    return VIRTUAL_THREADS_SUPPORTED;
  }

  private static int parseJavaVersion() {
    String version = System.getProperty("java.specification.version", "17");
    try {
      return Integer.parseInt(version);
    } catch (NumberFormatException e) {
      return 17;
    }
  }

  private static boolean checkVirtualThreads() {
    try {
      Thread.class.getMethod("ofVirtual");
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }
}
