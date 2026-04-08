# DDIA: Designing Data-Intensive Applications — Practical Examples

A collection of hands-on implementations exploring distributed systems patterns from *Designing Data-Intensive Applications* by Martin Kleppmann. Each module is a self-contained project demonstrating a specific architectural concept using real infrastructure.

## Modules

| Module | Pattern | Infrastructure |
|---|---|---|
| [caching](./caching) | Cache-aside with TTL | Redis + PostgreSQL |
| [kafka](./kafka) | Pub-sub with consumer groups | Kafka + ZooKeeper |
| [message-streams](./message-streams) | High-throughput stream processing | Kafka + ZooKeeper |
| [rabbitmq](./rabbitmq) | Reliable message queuing with DLX | RabbitMQ |
| [5. Circuit Balancers](./5.%20Circuit%20Balancers) | Distributed circuit breaker | Redis + microservices |
| [load-balancers](./load-balancers) | Load balancing algorithms | (documentation only) |

## Tech Stack

- **Runtime**: [Bun](https://bun.sh) v1.3.2+
- **Language**: TypeScript v5+
- **Framework**: Express.js v5
- **Messaging**: KafkaJS, amqplib (RabbitMQ)
- **Infrastructure**: Docker + Docker Compose

## Prerequisites

- [Bun](https://bun.sh) v1.3.2+
- [Docker](https://www.docker.com) + Docker Compose

## Quick Start

Each module is independent. Navigate to the module directory, start its infrastructure, and run the service:

```bash
cd <module>
bun install
docker-compose up -d
bun run start   # or see module README for specific commands
```

Refer to each module's README for detailed setup, endpoints, and usage examples.

## Patterns Covered

### Cache-Aside (Lazy Loading)
The [caching](./caching) module demonstrates checking Redis before hitting PostgreSQL, populating the cache on miss, and expiring entries with TTL. Useful for read-heavy workloads where stale data is acceptable.

### Pub-Sub Messaging
The [kafka](./kafka) and [message-streams](./message-streams) modules show decoupled producers and consumers via Kafka topics. Multiple consumers in different consumer groups can each receive every message independently.

### Reliable Message Queuing
The [rabbitmq](./rabbitmq) module covers publisher confirms, manual consumer acknowledgements, dead-letter exchanges (DLX) for failed messages, and exponential backoff reconnection.

### Distributed Circuit Breaker
The [5. Circuit Balancers](./5.%20Circuit%20Balancers) module implements the circuit breaker pattern across two microservices (post-service and profile-service), using Redis pub-sub to synchronize circuit state so all instances agree on whether a downstream service is healthy.

### Load Balancing Algorithms
The [load-balancers](./load-balancers) module documents Round Robin, Least Connections, and IP Hash strategies with trade-off analysis.

## Repository Structure

```
.
├── caching/              # Express API + Redis cache + PostgreSQL
├── kafka/                # Kafka producer + multiple consumers
├── message-streams/      # Batch producer + streaming consumer
├── rabbitmq/
│   ├── producer/         # Publisher with confirm channel
│   └── consumer/         # Consumer with manual acks + DLX
├── 5. Circuit Balancers/
│   ├── post-service/     # Calls profile-service via circuit breaker
│   └── profile-service/  # Downstream service
└── load-balancers/       # Algorithm documentation
```
