# Distributed Systems & Backend Patterns Repository

A comprehensive collection of educational examples demonstrating modern distributed systems concepts, message queuing patterns, caching strategies, and microservice architectures. All projects are built with **Bun**, a fast all-in-one JavaScript runtime.

## 📚 Table of Contents

- [Overview](#overview)
- [Projects](#projects)
- [Quick Start](#quick-start)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Running Individual Projects](#running-individual-projects)
- [Project Structure](#project-structure)
- [Learning Path](#learning-path)

## Overview

This repository contains hands-on implementations of key distributed systems concepts, including:

- **Caching patterns** with Redis and database optimization
- **Message queuing** with RabbitMQ and Kafka
- **Load balancing** algorithms (Round Robin, Least Connections, IP Hash)
- **Circuit breaker patterns** for fault tolerance
- **Real-time event streaming** with Kafka and ZooKeeper
- **Producer-consumer patterns** for asynchronous processing

Each project includes detailed documentation explaining the concepts, implementation details, and usage patterns.

## 🎯 Projects

### 1. **Caching** (`/caching`)

Demonstrates HTTP caching patterns using Redis and PostgreSQL.

**Key Features:**
- Redis client integration with `ioredis`
- PostgreSQL connection pooling
- Cache-aside pattern implementation
- TTL management for cache invalidation
- Endpoints: `/ping`, `/users`, `/users/:id`

**Tech Stack:** Bun, Redis, PostgreSQL, Express.js

**Learn:** Cache hit/miss patterns, cache invalidation strategies, database optimization

---

### 2. **Kafka Message Streaming** (`/message-streams`)

High-performance distributed message streaming using Apache Kafka and ZooKeeper.

**Key Features:**
- Kafka producer and consumer implementation
- Topic-based pub-sub architecture
- Consumer groups for parallel processing
- Partition-based message ordering
- Kafka-UI for visual monitoring

**Tech Stack:** Bun, Kafka, ZooKeeper, KafkaJS, Docker Compose

**Learn:** Event streaming, topic partitioning, consumer groups, offset management

---

### 3. **RabbitMQ** (`/rabbitmq`)

Robust message queue implementation with RabbitMQ.

**Subdirectories:**
- **Producer** (`/rabbitmq/producer`) — publishes messages with confirms
- **Consumer** (`/rabbitmq/consumer`) — consumes messages with manual acks

**Key Features:**
- Persistent message publishing
- Publisher confirms for reliability
- Manual acknowledgment (safe consuming)
- Dead-letter exchange patterns
- Idempotent message processing

**Tech Stack:** Bun, RabbitMQ, amqplib

**Learn:** Message reliability, consumer patterns, DLX and retry strategies, idempotency

---

### 4. **Load Balancers** (`/load-balancers`)

Overview and comparison of load balancing algorithms.

**Algorithms Covered:**
- **Round Robin** — Cycles through servers sequentially. Ideal when servers are equal in capacity.
- **Least Connections** — Directs requests to the server with fewest active connections. Better for long-running requests.
- **IP Hash** — Routes same client IP to same server (sticky sessions). Useful for in-memory session state.

**Learn:** Server selection strategies, session affinity, connection distribution

---

### 5. **Circuit Breakers** (`/5. Circuit Balancers`)

Demonstrates fault-tolerant service communication.

**Subdirectories:**
- **Post Service** (`/5. Circuit Balancers/post-service`)
- **Profile Service** (`/5. Circuit Balancers/profile-service`)

**Key Concepts:**
- Service-to-service communication with fallback
- Circuit breaker state machine (Open, Closed, Half-Open)
- Failure detection and graceful degradation

**Tech Stack:** Bun, Node.js

**Learn:** Fault tolerance, graceful degradation, failure handling

---

### 6. **Kafka Direct** (`/kafka`)

Basic Kafka implementation example.

**Tech Stack:** Bun, Kafka, KafkaJS

**Learn:** Kafka producer/consumer basics, topic management

---

## 🚀 Quick Start

### Prerequisites

- **[Bun](https://bun.sh)** (v1.3.2 or later)
- **[Docker & Docker Compose](https://www.docker.com/)** (for Kafka, Redis, RabbitMQ, PostgreSQL)
- **Node.js 18+** (optional, if running with Node instead of Bun)

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repo-url>
   cd repo
   ```

2. **Navigate to a project:**
   ```bash
   cd caching
   ```

3. **Install dependencies:**
   ```bash
   bun install
   ```

## 💻 Technology Stack

| Technology    | Purpose                                | Used In                                     |
| ------------- | -------------------------------------- | ------------------------------------------- |
| **Bun**       | JavaScript runtime (faster than Node)  | All projects                                |
| **TypeScript** | Type-safe development                  | All projects                                |
| **Redis**     | In-memory caching                      | Caching, RabbitMQ (optional persistence)   |
| **PostgreSQL** | Relational database                    | Caching                                     |
| **Kafka**     | Distributed message broker             | Message Streams, Kafka                      |
| **ZooKeeper** | Cluster coordination                   | Message Streams                             |
| **RabbitMQ**  | Message queue broker                   | RabbitMQ projects                           |
| **Docker**    | Containerization & orchestration       | Infrastructure for all stateful services    |

## 📋 Prerequisites

Before running projects, ensure you have:

1. **Bun installed:**
   ```bash
   curl -fsSL https://bun.sh/install | bash
   ```

2. **Docker running:**
   ```bash
   docker --version
   docker-compose --version
   ```

3. **Basic understanding of:**
   - HTTP and REST APIs
   - JSON serialization
   - Asynchronous programming (promises, async/await)

## 🔧 Running Individual Projects

### Caching Example

```bash
cd caching

# Start infrastructure (Redis + PostgreSQL)
docker-compose up -d

# Install dependencies
bun install

# Run the server
bun run index.ts

# Test endpoints
curl http://localhost:3000/ping
curl http://localhost:3000/users
```

**See** `caching/README.md` for detailed API documentation.

---

### Message Streams (Kafka)

```bash
cd message-streams

# Start Kafka + ZooKeeper + Kafka-UI
docker-compose up -d

bun install

# Terminal 1: Start server (producer)
bun run src/server.ts

# Terminal 2: Start consumer
bun run src/consumer.ts

# Terminal 3: Publish a message
curl -X POST http://localhost:3000/order \
  -H "Content-Type: application/json" \
  -d '{"orderId": 123, "amount": 99.99}'

# Monitor: Open http://localhost:8080 in browser
```

**See** `message-streams/README.md` for full documentation.

---

### RabbitMQ (Producer & Consumer)

```bash
cd rabbitmq

# Start RabbitMQ
docker-compose up -d

cd producer
bun install
bun run index.ts

cd ../consumer
bun install
bun run index.ts
```

**See** `rabbitmq/README.md`, `rabbitmq/producer/README.md`, and `rabbitmq/consumer/README.md` for details.

---

### Circuit Breakers

```bash
cd "5. Circuit Balancers"

# Terminal 1: Start profile service
cd profile-service
bun install
bun run index.ts

# Terminal 2: Start post service (depends on profile)
cd ../post-service
bun install
bun run index.ts
```

**See** the service READMEs for architecture details.

---

## 📁 Project Structure

```
repo/
├── caching/                      # Redis + PostgreSQL caching patterns
│   ├── index.ts
│   ├── redis.ts
│   ├── db.ts
│   ├── docker-compose.yml
│   └── README.md
│
├── message-streams/              # Kafka streaming with ZooKeeper
│   ├── src/
│   │   ├── server.ts            # HTTP producer
│   │   ├── consumer.ts          # Message consumer
│   │   ├── producer.ts
│   │   └── kafka.ts
│   ├── docker-compose.yml
│   └── README.md
│
├── kafka/                         # Basic Kafka example
│   ├── index.ts
│   └── package.json
│
├── rabbitmq/                      # RabbitMQ patterns
│   ├── producer/
│   │   ├── index.ts
│   │   ├── README.md
│   │   └── package.json
│   ├── consumer/
│   │   ├── index.ts
│   │   ├── README.md
│   │   └── package.json
│   ├── docker-compose.yml
│   └── README.md
│
├── load-balancers/               # Load balancing algorithms
│   └── README.md
│
├── 5. Circuit Balancers/          # Circuit breaker pattern
│   ├── post-service/
│   │   ├── index.ts
│   │   ├── README.md
│   │   └── package.json
│   └── profile-service/
│       ├── index.ts
│       ├── README.md
│       └── package.json
│
└── README.md                      # This file
```

## 🎓 Learning Path

**Beginner:**
1. Start with **Load Balancers** — understand server selection theory
2. Move to **Caching** — learn cache patterns with hands-on code
3. Explore **Circuit Breakers** — understand fault tolerance

**Intermediate:**
4. Study **RabbitMQ** — learn reliable message queuing
5. Understand producer-consumer patterns in depth
6. Implement retry and dead-letter strategies

**Advanced:**
7. Dive into **Kafka & Message Streams** — distributed event streaming
8. Learn about partitioning, consumer groups, and offset management
9. Explore scaling and performance tuning

**Recommended Reading:**
- [Designing Data-Intensive Applications](https://dataintensive.net/) (DDIA) — foundational concepts
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [RabbitMQ Tutorials](https://www.rabbitmq.com/getstarted.html)

## 🛠️ Development Tips

### Running with Node.js instead of Bun

If you prefer Node.js:
```bash
cd caching
npm install  # or yarn
npm run dev  # uses ts-node or tsx
```

### Debugging

Enable verbose logging:
```bash
# Kafka
export DEBUG=kafkajs:*
bun run src/server.ts

# RabbitMQ
export DEBUG=amqplib:*
bun run index.ts
```

### Stopping Services

```bash
# Stop all Docker services
docker-compose down

# Remove volumes (WARNING: deletes data)
docker-compose down -v
```

### Common Issues

| Issue | Solution |
| --- | --- |
| Port already in use | Change port in code or `docker-compose.yml` |
| Connection refused | Ensure Docker containers are running: `docker ps` |
| Module not found | Run `bun install` in the project directory |
| Bun not found | Install Bun: `curl -fsSL https://bun.sh/install \| bash` |

## 📖 Additional Resources

### Per-Project Documentation

Each project has a detailed `README.md`:
- `caching/README.md` — Cache patterns, endpoints, troubleshooting
- `message-streams/README.md` — Kafka architecture, usage patterns, debugging
- `rabbitmq/README.md` — RabbitMQ topology, best practices
- `load-balancers/README.md` — Algorithm comparison

### External Links

- [Bun Documentation](https://bun.sh/docs)
- [Docker Compose Docs](https://docs.docker.com/compose/)
- [Apache Kafka](https://kafka.apache.org/)
- [RabbitMQ](https://www.rabbitmq.com/)
- [Redis](https://redis.io/)
- [PostgreSQL](https://www.postgresql.org/)

## 💡 Contributing

To add or improve projects:

1. Follow the existing structure: TypeScript, Bun, Docker Compose
2. Include a detailed `README.md` with concepts and usage
3. Add inline comments explaining non-obvious patterns
4. Ensure reproducibility with `docker-compose.yml`

## 📝 License

[License information — update as needed]

---

**Happy learning! 🚀** Start with any project that interests you and dive into the code.
