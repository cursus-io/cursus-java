package io.cursus.client.util;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates thread pool executors with Virtual Threads support. On Java 21+, automatically uses
 * virtual thread-per-task executor. On Java 17-20, falls back to a fixed thread pool.
 */
public final class ExecutorFactory {

  private static final Logger log = LoggerFactory.getLogger(ExecutorFactory.class);

  private ExecutorFactory() {}

  public static ExecutorService create(int poolSize, String name) {
    if (RuntimeDetector.supportsVirtualThreads()) {
      try {
        Method method = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
        ExecutorService executor = (ExecutorService) method.invoke(null);
        log.info("Using Virtual Thread executor for '{}'", name);
        return executor;
      } catch (Exception e) {
        log.warn("Failed to create Virtual Thread executor, falling back to fixed pool", e);
      }
    }
    log.info("Using fixed thread pool (size={}) for '{}'", poolSize, name);
    return Executors.newFixedThreadPool(
        poolSize,
        r -> {
          Thread t = new Thread(r);
          t.setName(name + "-" + t.getId());
          t.setDaemon(true);
          return t;
        });
  }
}
