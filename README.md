# Designing Data-Intensive Applications (DDIA) — Code Examples

A collection of hands-on code examples demonstrating key patterns and architectures from *Designing Data-Intensive Applications*. Each directory contains self-contained, runnable examples with Docker Compose setup for local experimentation.

## Table of Contents

- [Overview](#overview)
- [Directory Structure](#directory-structure)
- [Technologies](#technologies)
- [Quick Start](#quick-start)
- [Examples](#examples)

## Overview

This repository provides practical, production-minded implementations of:

- **Message Streaming & Queuing** — RabbitMQ, Kafka, and real-time event processing
- **Caching Strategies** — Redis integration, cache invalidation, TTL management
- **Load Balancing** — Round-robin, least connections, IP hash algorithms
- **Resilience Patterns** — Circuit breakers and fault tolerance
- **Distributed Architecture** — Multi-service coordination and message flows

Each example is designed to be:
- **Code-first** — understand patterns through concrete implementations
- **Runnable locally** — Docker Compose files for easy setup
- **Production-minded** — includes error handling, idempotency, observability
- **Well-documented** — detailed READMEs explaining the "why" behind patterns

## Directory Structure

```
.
├── rabbitmq/              # Producer-consumer patterns with RabbitMQ
│   ├── producer/          # Message publisher implementation
│   ├── consumer/          # Message consumer with retry logic
│   └── README.md          # RabbitMQ topology and best practices
├── message-streams/       # Kafka-based event streaming
│   ├── src/               # TypeScript implementation
│   └── README.md          # Kafka architecture and configuration
├── kafka/                 # Additional Kafka examples
├── caching/               # Redis caching with Postgres fallback
│   ├── index.ts           # HTTP server with cache layer
│   ├── redis.ts           # Redis client configuration
│   ├── db.ts              # Postgres connection pool
│   └── README.md          # Caching strategy and examples
├── load-balancers/        # Load balancing algorithms
│   └── README.md          # Round-robin, least connections, IP hash
└── 5. Circuit Balancers/  # Fault tolerance with circuit breakers
    ├── post-service/      # Service A implementation
    ├── profile-service/   # Service B implementation
    └── docker-compose.yml # Multi-service orchestration
```

## Technologies

- **Languages:** TypeScript, JavaScript
- **Runtimes:** Node.js, Bun
- **Message Brokers:** RabbitMQ, Apache Kafka
- **Databases:** PostgreSQL
- **Cache:** Redis
- **Containerization:** Docker & Docker Compose

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Node.js/Bun (for local development)
- git

### Running an Example

1. **Navigate to a directory:**
   ```bash
   cd caching
   ```

2. **Start services:**
   ```bash
   docker-compose up -d
   ```

3. **Install and run:**
   ```bash
   bun install
   bun run index.ts
   ```

4. **Test endpoints:**
   ```bash
   curl http://localhost:3000/ping | jq
   ```

5. **View logs and stop:**
   ```bash
   docker-compose logs -f
   docker-compose down
   ```

## Examples

### RabbitMQ — Safe Publish & Consume

Learn patterns for reliable message delivery:
- **Publisher confirms** — ensure broker accepted the message
- **Manual acks** — process only after successful handling
- **Dead-letter queues** — handle failed messages
- **Idempotency** — deduplicate retried messages

👉 [Read rabbitmq/README.md](./rabbitmq/README.md)

### Message Streams — Kafka Event Processing

High-performance, ordered message processing:
- **Topic partitioning** — parallel consumption by key
- **Consumer groups** — scale consumers independently
- **Offset management** — exactly-once semantics
- **Rebalancing** — handle consumer failures gracefully

👉 [Read message-streams/README.md](./message-streams/README.md)

### Caching — Redis with Database Fallback

Optimize latency with intelligent caching:
- **Cache-aside pattern** — check cache, fallback to DB, populate cache
- **TTL expiration** — automatic eviction of stale data
- **Cache hits/misses** — observe performance impact
- **Invalidation strategies** — manual and automatic refresh

👉 [Read caching/README.md](./caching/README.md)

### Load Balancers — Distribution Algorithms

Distribute traffic across servers:
- **Round-robin** — fair, simple distribution
- **Least connections** — route to server with fewest active requests
- **IP hash** — sticky sessions for stateful services

👉 [Read load-balancers/README.md](./load-balancers/README.md)

### Circuit Breakers — Fault Tolerance

Prevent cascading failures in distributed systems:
- **State machine** — open/closed/half-open states
- **Fallback logic** — graceful degradation
- **Recovery patterns** — how to restart after failure

👉 [Explore 5. Circuit Balancers/](./5.%20Circuit%20Balancers/)

## Learning Path

**Beginner:**
1. Start with [caching/README.md](./caching/README.md) — single service, Redis + PostgreSQL
2. Move to [load-balancers/README.md](./load-balancers/README.md) — understand traffic distribution

**Intermediate:**
3. Study [rabbitmq/README.md](./rabbitmq/README.md) — asynchronous messaging, at-least-once delivery
4. Explore [message-streams/README.md](./message-streams/README.md) — ordered, partitioned streaming

**Advanced:**
5. Implement [5. Circuit Balancers/](./5.%20Circuit%20Balancers/) — multi-service resilience

## Common Patterns Across Examples

### Message Envelope

All async examples use a consistent envelope:
```json
{
  "id": "uuid",
  "type": "event.type",
  "source": "service-name",
  "timestamp": 1680000000000,
  "payload": { /* event-specific data */ }
}
```

### Error Handling

- **Transient errors** — retry with exponential backoff
- **Persistent errors** — move to dead-letter queue or DLQ
- **Logging** — include correlation IDs for tracing

### Observability

- **Metrics** — track throughput, latency, queue depth
- **Logs** — structured logging with context
- **Traces** — correlation IDs across service boundaries

## Contributing

- Each example is self-contained; feel free to fork and extend
- Update READMEs when adding new patterns or dependencies
- Include Docker Compose for reproducibility

## References

These examples are inspired by concepts from:
- *Designing Data-Intensive Applications* by Martin Kleppmann
- Production patterns from Kafka, RabbitMQ, and Redis ecosystems
- Real-world distributed systems challenges

## License

Open for learning and reference.
