# Virtual Threads

The Cursus Java Client automatically uses Java 21 Virtual Threads when they are available, and falls back to a conventional fixed thread pool on Java 17–20. No configuration is required.

## How It Works

The `ExecutorFactory` class is the single place where all thread pools in the library are created. At startup it asks `RuntimeDetector.supportsVirtualThreads()` whether the JVM supports Virtual Threads, and then either creates a virtual-thread-per-task executor or a fixed-size daemon thread pool.

| Component | Java 17–20 | Java 21+ |
|---|---|---|
| Producer flush executor (`cursus-producer-flush`) | Fixed thread pool, size = `maxInflightRequests` | `Executors.newVirtualThreadPerTaskExecutor()` |
| Consumer worker executor (`cursus-consumer`) | Fixed thread pool, size = `availableProcessors` | `Executors.newVirtualThreadPerTaskExecutor()` |
| Spring `@CursusListener` executor (`cursus-listener`) | Fixed thread pool, size = `availableProcessors` | `Executors.newVirtualThreadPerTaskExecutor()` |
| Linger scheduler (`cursus-linger`) | Single platform daemon thread | Single platform daemon thread (scheduler, not blocking) |
| Commit scheduler (`cursus-commit`) | Single platform daemon thread | Single platform daemon thread |
| Heartbeat scheduler (`cursus-heartbeat`) | Single platform daemon thread | Single platform daemon thread |

The linger, commit, and heartbeat schedulers are `ScheduledExecutorService` instances backed by a single thread. They perform short, non-blocking work on each tick and are not substituted with virtual threads.

## Detection Mechanism

`RuntimeDetector` checks for Virtual Thread support at class load time using reflection:

```java
private static boolean checkVirtualThreads() {
    try {
        Thread.class.getMethod("ofVirtual");
        return true;
    } catch (NoSuchMethodException e) {
        return false;
    }
}
```

`Thread.ofVirtual()` was introduced in Java 21. If the method exists, Virtual Threads are supported. This approach avoids a hard compile-time dependency on Java 21 API.

`ExecutorFactory` then invokes `Executors.newVirtualThreadPerTaskExecutor()` via reflection:

```java
Method method = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
ExecutorService executor = (ExecutorService) method.invoke(null);
```

This means the library's bytecode compiles and runs on Java 17 while still taking full advantage of Virtual Threads when the JVM is Java 21 or later.

## No Configuration Required

Virtual Threads are enabled automatically. There is no `cursus.virtual-threads.enabled` property or any other setting to toggle. The behavior is entirely determined by the JVM version at runtime.

## Benefits

Running on Java 21+ with Virtual Threads provides the following advantages:

- **Higher concurrency without more platform threads** — each in-flight batch send or consumer partition loop occupies a virtual thread that parks on I/O rather than blocking a platform thread.
- **Simpler backpressure model** — the virtual-thread-per-task executor allows many more concurrent tasks than a fixed pool without exhausting OS thread limits.
- **Reduced latency under load** — no thread starvation when many partitions are active simultaneously.
- **Lower memory per thread** — virtual threads have a much smaller stack footprint than platform threads, which matters when running many consumers in the same JVM.

## Compile Target

The entire codebase — including `ExecutorFactory` and `RuntimeDetector` — is compiled with `sourceCompatibility = JavaVersion.VERSION_17` and `targetCompatibility = JavaVersion.VERSION_17`. This guarantees the library runs on any Java 17+ JVM. All Java 21 API access goes through reflection so there are no `ClassNotFoundException` errors on older runtimes.
