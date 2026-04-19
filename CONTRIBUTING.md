# Contributing to Cursus Java Client

Thank you for your interest in contributing. This document covers how to set up a development environment, the project structure, and the contribution guidelines.

## Development Setup

### Prerequisites

- JDK 17 or later (JDK 21 recommended)
- Git

### Clone and build

```bash
git clone https://github.com/cursus-io/cursus-java.git
cd cursus-java
./gradlew build
```

### Run all tests

```bash
./gradlew test
```

### Run tests for a specific module

```bash
./gradlew :cursus-client:test
./gradlew :cursus-spring-boot-starter:test
```

### Build without running tests

```bash
./gradlew assemble
```

## Project Structure

```
cursus-java/
├── cursus-client/                  Core library (Netty + pure Java)
│   └── src/main/java/io/cursus/client/
│       ├── compression/            CursusCompressor interface, GzipCompressor, CompressionRegistry
│       ├── config/                 CursusProducerConfig, CursusConsumerConfig, Acks, ConsumerMode
│       ├── connection/             ConnectionManager (Netty), frame encoder/decoder, client handler
│       ├── consumer/               CursusConsumer, PartitionConsumer
│       ├── exception/              CursusException hierarchy
│       ├── message/                CursusMessage, AckResponse
│       ├── producer/               CursusProducer, PartitionBuffer, BatchState
│       ├── protocol/               ProtocolEncoder, ProtocolDecoder, CommandBuilder
│       └── util/                   ExecutorFactory, RuntimeDetector, FnvHash, Backoff
│
├── cursus-spring-boot-starter/     Spring Boot auto-configuration
│   └── src/main/java/io/cursus/spring/
│       ├── annotation/             @CursusListener, CursusListenerRegistrar
│       └── autoconfigure/          CursusAutoConfiguration, CursusProperties
│
├── cursus-examples/
│   ├── standalone/                 5 runnable standalone examples
│   └── spring-boot/                Spring Boot REST app example
│
└── docs/                           Additional documentation
```

## Guidelines

### Java version target

All production code in `cursus-client` and `cursus-spring-boot-starter` must compile against Java 17 (`sourceCompatibility = JavaVersion.VERSION_17`). If you need Java 21 APIs (e.g., `Thread.ofVirtual()`), access them via reflection exactly as `ExecutorFactory` and `RuntimeDetector` do — do not raise the source/target compatibility level.

### Tests required

Every new feature or bug fix must be accompanied by at least one unit test. Tests live alongside the source code under `src/test/java`. The project uses JUnit 5.

### Javadoc

Public API classes and methods must have English-language Javadoc. Follow the style already present in the codebase: a short summary sentence, `@param` tags for non-obvious parameters, and a cross-reference to the Go SDK equivalent where relevant (e.g., `Maps to Go SDK's PublisherConfig in config.go`).

### Code style

- Follow the formatting and naming conventions already used in each module.
- Use Lombok (`@Data`, `@Builder`) for plain data classes, matching the existing pattern.
- Prefer `AutoCloseable` for any resource-owning class so callers can use try-with-resources.
- Keep classes focused: each class should do one thing.

### Commit messages

Write commit messages in English, in the imperative mood, with a short subject line (72 characters or fewer). Add a blank line followed by a more detailed body when the change needs explanation.

## Reporting Issues

Open an issue on the GitHub repository with:

1. A clear description of the problem or feature request.
2. The Java version and OS you are using.
3. A minimal, self-contained code sample that reproduces the issue (if applicable).
4. The full stack trace (if applicable).
