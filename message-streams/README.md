# Message-Streams Service

A high-performance, scalable message streaming application built with **Kafka**, **ZooKeeper**, and **Bun**. This service demonstrates real-time event processing patterns using publish-subscribe architecture.

## Table of Contents

- [What Is This?](#what-is-this)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Components](#components)
- [Usage](#usage)
- [How It Works](#how-it-works)
- [Key Concepts](#key-concepts)
- [Development](#development)

## What Is This?

This is a demonstration of **distributed message streaming** using Apache Kafka. It showcases:

- **Real-time event publishing** via HTTP endpoints
- **Asynchronous event consumption** with consumer groups
- **Durable message storage** with topic-based organization
- **Scalable pub-sub patterns** for decoupled services
- **Partition-based ordering** for event processing

### Real-World Analogy

Think of Kafka like a **newspaper publishing system**:

- **Producers** (like journalists) write articles (messages) to specific sections (topics)
- **Brokers** (like the printing press) store and manage all articles
- **Consumers** (like readers) subscribe to sections they care about and read articles at their own pace
- **ZooKeeper** (like the editorial board) coordinates and manages all operations

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Message-Stream Service                       │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────┐          ┌──────────────────┐
│  HTTP Server     │          │  Kafka Producer  │
│  (Port 3000)     │────────>│  (sends events)  │
│  /order endpoint │          └──────────────────┘
└──────────────────┘                  │
                                      │
                                      ▼
                        ┌─────────────────────────┐
                        │   Kafka Broker          │
                        │  (Port 9092)            │
                        │                         │
                        │  Topics:                │
                        │  - orders               │
                        │  - bottles              │
                        └─────────────────────────┘
                                      │
                        ┌─────────────┴──────────────┐
                        │                            │
                        ▼                            ▼
                ┌──────────────────┐      ┌──────────────────┐
                │ Consumer Group 1  │      │ Consumer Group 2  │
                │ (processes data)  │      │ (processes data)  │
                └──────────────────┘      └──────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                      ZooKeeper                                    │
│  (Cluster coordination, broker management, consumer offsets)      │
│  (Port 2181)                                                      │
└──────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Client sends HTTP POST** → `/order` endpoint with JSON payload
2. **Producer publishes** → Message to Kafka topic
3. **Kafka stores** → Message in partitioned log (durably)
4. **Consumers subscribe** → To topics and pull messages
5. **Processing** → Each consumer processes messages at its own pace
6. **Offset tracking** → ZooKeeper tracks consumption progress

## Technology Stack

| Component      | Version | Purpose                                      |
| -------------- | ------- | -------------------------------------------- |
| **Kafka**      | 7.5.0   | Distributed message broker                   |
| **ZooKeeper**  | 7.5.0   | Cluster coordination and metadata management |
| **Bun**        | Latest  | Fast JavaScript runtime (TypeScript support) |
| **KafkaJS**    | 2.2.4   | Node.js/Bun Kafka client library             |
| **Kafka-UI**   | Latest  | Web UI for monitoring Kafka (port 8080)      |
| **TypeScript** | 5+      | Type-safe development                        |
| **Docker**     | -       | Container orchestration                      |

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Bun installed ([bun.sh](https://bun.sh))
- Node.js 18+ (or Bun as runtime)

### Setup

1. **Clone and navigate:**

   ```bash
   cd message-streams
   ```

2. **Install dependencies:**

   ```bash
   bun install
   ```

3. **Start infrastructure (Kafka + ZooKeeper + Kafka-UI):**

   ```bash
   docker-compose up -d
   ```

4. **Verify services are running:**

   ```bash
   docker-compose ps
   ```

   You should see:
   - `zookeeper` running on port 2181
   - `kafka` running on port 9092
   - `kafka-ui` running on port 8080

### Running the Service

**Terminal 1 - Start the HTTP Server (Producer):**

```bash
bun run src/server.ts
```

Output: `Server running on http://localhost:3000`

**Terminal 2 - Start the Consumer:**

```bash
bun run src/consumer.ts
```

**Terminal 3 - Publish a message:**

```bash
curl -X POST http://localhost:3000/order \
  -H "Content-Type: application/json" \
  -d '{"orderId": 123, "amount": 99.99, "customer": "Alice"}'
```

You should see the message appear in the consumer terminal.

### Monitor in Web UI

Open [http://localhost:8080](http://localhost:8080) to see:

- Kafka clusters and brokers
- Topics and partitions
- Messages flowing through topics
- Consumer groups and lag
- Offset tracking

### Cleanup

```bash
docker-compose down
```

## Project Structure

```
message-streams/
├── src/
│   ├── kafka.ts           # Kafka client configuration
│   ├── server.ts          # HTTP server with producer
│   ├── producer.ts        # Standalone producer example
│   └── consumer.ts        # Consumer example
├── docker-compose.yml     # Infrastructure (Kafka, ZooKeeper, Kafka-UI)
├── package.json           # Dependencies and scripts
├── tsconfig.json          # TypeScript configuration
└── README.md              # This file
```

## Components

### 1. Kafka Configuration (`src/kafka.ts`)

```typescript
import { Kafka } from "kafkajs";

export const kafka = new Kafka({
  clientId: "bun-app",
  brokers: ["localhost:9092"],
});
```

**Purpose:** Centralized Kafka client setup. All producers and consumers use this shared configuration.

**Key Points:**

- `clientId`: Identifies this application to the Kafka cluster
- `brokers`: List of Kafka broker addresses (can be clustered for HA)

---

### 2. HTTP Server with Producer (`src/server.ts`)

```typescript
import { kafka } from "./kafka";

const producer = kafka.producer();
await producer.connect();

Bun.serve({
  port: 3000,
  async fetch(req) {
    if (req.method === "POST" && new URL(req.url).pathname === "/order") {
      const body = await req.json();
      await producer.send({
        topic: "orders",
        messages: [{ value: JSON.stringify(body) }],
      });
      return new Response("Published!", { status: 200 });
    }
    return new Response("Not found", { status: 404 });
  },
});
```

**Purpose:** Exposes HTTP endpoint to publish messages to Kafka.

**Behavior:**

- Listens on `http://localhost:3000`
- `POST /order` → publishes JSON payload to `orders` topic
- Returns `200 OK` if message published successfully

**Use Case:** REST API that decouples client requests from processing (fire-and-forget pattern).

---

### 3. Standalone Producer (`src/producer.ts`)

```typescript
import { kafka } from "./kafka";

const producer = kafka.producer();
await producer.connect();

await producer.send({
  topic: "bottles",
  messages: [
    {
      key: "bottle-1",
      value: JSON.stringify({ id: 1, item: "milton", qty: 2 }),
    },
    {
      key: "bottle-2",
      value: JSON.stringify({ id: 2, item: "coca-cola", qty: 5 }),
    },
  ],
});

await producer.disconnect();
```

**Purpose:** Batch publishing example; produces messages to `bottles` topic.

**Key Features:**

- Multiple messages in single batch
- Message keys for partitioning (same key → same partition)
- Graceful disconnect after publishing

---

### 4. Consumer (`src/consumer.ts`)

```typescript
import { kafka } from "./kafka";

const consumer = kafka.consumer({ groupId: "Bottles group" });
await consumer.connect();
await consumer.subscribe({ topic: "bottles", fromBeginning: true });
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({
      partition,
      offset: message.offset,
      value: message.value?.toString(),
    });
  },
});
```

**Purpose:** Consumes and processes messages from Kafka topics.

**Key Features:**

- `groupId`: Consumer group for coordinated consumption
- `fromBeginning: true`: Reads all historical messages (not just new ones)
- `eachMessage`: Callback for per-message processing
- Automatic offset tracking (ZooKeeper remembers where we left off)

---

## How It Works

### Message Publishing Flow

1. **Client sends HTTP request:**

   ```json
   POST /order
   {
     "orderId": 123,
     "amount": 99.99,
     "customer": "Alice"
   }
   ```

2. **Producer serializes and sends:**
   - JSON → string
   - Sends to `orders` topic with no key (round-robin partitioning)

3. **Kafka broker receives:**
   - Validates schema (optional)
   - Writes to partition log (durable)
   - Replicates (if multi-broker)
   - Acknowledges to producer

4. **HTTP response returns:**
   ```
   200 OK - Published!
   ```

### Message Consumption Flow

1. **Consumer subscribes:**
   - Joins consumer group `"Bottles group"`
   - ZooKeeper coordinates: assigns partitions to consumer instances

2. **Consumer fetches messages:**
   - Pulls from assigned partitions
   - Processes with `eachMessage` callback

3. **Consumer commits offset:**
   - Records last read position in ZooKeeper
   - Enables recovery after restart
   - Supports exactly-once semantics (with idempotence)

---

## Key Concepts

### Topics and Partitions

A **topic** is a named log where messages are appended. Topics are divided into **partitions** for parallelism:

```
Topic: "orders"
├── Partition 0: [msg1, msg2, msg3, ...]
├── Partition 1: [msg4, msg5, msg6, ...]
└── Partition 2: [msg7, msg8, msg9, ...]
```

**Benefits:**

- Parallel processing (one consumer per partition)
- Durability (stored on broker disk)
- Ordering (within partition, not across)

### Consumer Groups

Multiple consumers with same `groupId` form a **consumer group**:

- Partitions are divided among consumers
- Only ONE consumer reads from each partition (no duplicate processing)
- Enables horizontal scaling

Example with 3 consumers and 3 partitions:

```
Consumer 1 → Partition 0
Consumer 2 → Partition 1
Consumer 3 → Partition 2
```

### Offsets and Commits

An **offset** is the position in a partition:

- Each message has an offset (0, 1, 2, ...)
- Consumers track their current offset
- **ZooKeeper stores offsets** for resuming after crash/restart
- Manual or auto-commit strategies available

### ZooKeeper's Role

ZooKeeper handles:

- **Broker coordination:** Which brokers are alive?
- **Leader election:** Which broker is partition leader?
- **Consumer group management:** Which partitions assigned to which consumers?
- **Offset storage:** Where did each consumer group leave off?
- **Configuration:** Topic creation, retention policies, etc.

### Message Keys

Optional key in `{ key, value }` determines partitioning:

- `key: null` → round-robin (random partition)
- `key: "user-123"` → same key always goes to same partition (maintains order for user)

---

## Usage Patterns

### Pattern 1: Request-Response Decoupling

```
[Web Client] → POST /order → [HTTP Server + Producer]
                                        ↓
                                   Kafka Topic
                                        ↓
                                   [Consumer]
                                        ↓
                          (Process order, update DB, send email)
```

**Benefit:** Client gets instant response; processing happens async.

### Pattern 2: Event Streaming

```
[Microservice A] → publishes events → [Kafka]
[Microservice B] → subscribes         ↓
[Microservice C] → subscribes         [all get same events]
```

**Benefit:** Loose coupling; new services can subscribe without modifying source.

### Pattern 3: Log Aggregation

```
[Service 1] ┐
[Service 2] ├→ [Kafka] → [Search/Analytics]
[Service 3] ┘
```

**Benefit:** Centralized log storage with high throughput.

---

## Development

### Adding New Topics

1. **Auto-create (enabled in docker-compose):**

   ```typescript
   await producer.send({
     topic: "new-topic-name",
     messages: [{ value: "test
   ```

   " }],
   });

   ```

   ```

2. **Manual via Kafka CLI:**
   ```bash
   docker exec kafka kafka-topics --create \
     --bootstrap-server localhost:9092 \
     --topic my-topic \
     --partitions 3 \
     --replication-factor 1
   ```

### Adding New Consumer Groups

```typescript
const consumer = kafka.consumer({ groupId: "my-new-group" });
await consumer.subscribe({ topic: "orders" });
await consumer.run({ eachMessage: ... });
```

Each consumer group independently tracks offsets (can replay messages).

### Debugging

**View topics:**

```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

**View consumer group lag:**

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group "Bottles group" \
  --describe
```

**Read messages (CLI):**

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bottles \
  --from-beginning
```

### Performance Tuning

| Parameter            | Impact                              | Example               |
| -------------------- | ----------------------------------- | --------------------- |
| `partitions`         | Parallelism; more = more throughput | 3-10 for medium scale |
| `replication-factor` | Durability; more = safer but slower | 3 for production      |
| `compression.type`   | Network bandwidth                   | `snappy` or `lz4`     |
| `batch.size`         | Throughput vs latency               | 16KB for balanced     |
| `linger.ms`          | Wait time before send               | 10ms for low-latency  |

---

## Common Issues and Solutions

| Issue                         | Cause                                       | Solution                                       |
| ----------------------------- | ------------------------------------------- | ---------------------------------------------- |
| `Unable to connect to broker` | Kafka not running                           | `docker-compose up -d`                         |
| `Consumer lag high`           | Processing slower than production           | Add more consumers or optimize code            |
| `Out of memory`               | Too many messages buffered                  | Reduce `fetch.max.bytes` or add consumers      |
| `Offset out of range`         | Topic retention policy deleted old messages | Use `fromBeginning: false` or adjust retention |

---

## Next Steps

1. **Explore Kafka-UI** → Understand topics, partitions, and message flow visually
2. **Add error handling** → Implement retry logic and dead-letter queues
3. **Scale consumers** → Run multiple consumer instances in same group
4. **Add persistence** → Save processed messages to database
5. **Implement transactions** → Use Kafka transactions for exactly-once semantics
6. **Set up monitoring** → Use Prometheus + Grafana for metrics

---

## Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [KafkaJS](https://kafka.js.org/)
- [ZooKeeper Guide](https://zookeeper.apache.org/doc/current/)
- [Confluent Kafka Tutorials](https://kafka-tutorials.confluent.io/)
- [DDIA: Designing Data-Intensive Applications](https://dataintensive.net/)

---

**Happy streaming! 🚀**
