ddia/rabbitmq/consumer/README.md

# RabbitMQ Consumer (ddia/rabbitmq/consumer)

This repository contains a lightweight, production-minded RabbitMQ consumer implemented for Bun/Node ecosystems. The focus of this README is on code: how the consumer connects to RabbitMQ, declares topology, handles messages, deals with errors, and performs graceful shutdown and reconnection.

Goals

- Implement a resilient, manual-ack consumer with retry/dead-letter handling.
- Keep message processing idempotent-friendly (at-least-once delivery).
- Provide clear examples for connection management, validation, logging, and shutdown.

---

Table of contents

- Overview & architecture
- Message format / schema
- Dependencies & install
- File layout
- Example consumer implementation (detailed)
- Reconnection & backoff
- Dead-letter & retry strategy
- Running locally (docker-compose)
- Observability & metrics
- Testing & troubleshooting
- Notes on idempotency & ordering

---

Overview & architecture

The consumer performs these responsibilities:

1. Establish a RabbitMQ connection and a confirm/channel.
2. Declare an exchange and a durable queue and bind them with routing key(s).
3. Set `prefetch` to provide backpressure to the broker.
4. Consume messages with manual acknowledgements (ack/nack).
5. Validate and parse message payloads; call an application `processMessage` handler.
6. On failure, use nack with requeue=false so message moves to a dead-letter exchange (DLX), or implement retry attempts using a retry queue/headers pattern.
7. Handle reconnection with exponential backoff and graceful shutdown on signals.

This yields an at-least-once guarantee — your processing must be idempotent if duplicates are possible.

---

Message format / schema

We recommend a JSON message envelope like:

```ddia/rabbitmq/consumer/README.md#L1-12
{
  "id": "uuid-v4",
  "type": "user.created",
  "timestamp": "2026-03-21T12:34:56Z",
  "payload": {
    "...": "domain specific"
  },
  "meta": {
    "attempt": 1
  }
}
```

A strict JSON schema (AJV-style) example:

```ddia/rabbitmq/consumer/README.md#L13-40
{
  "type": "object",
  "required": ["id","type","timestamp","payload"],
  "properties": {
    "id": { "type": "string", "minLength": 1 },
    "type": { "type": "string" },
    "timestamp": { "type": "string", "format": "date-time" },
    "payload": { "type": "object" },
    "meta": {
      "type": "object",
      "properties": {
        "attempt": { "type": "integer", "minimum": 1 }
      },
      "additionalProperties": true
    }
  },
  "additionalProperties": false
}
```

---

Dependencies & install

Example commands (using Bun):

```ddia/rabbitmq/consumer/README.md#L41-52
# from repository root: ddia/rabbitmq/consumer
bun install
# add runtime deps
bun add amqplib ajv pino
```

- `amqplib` — canonical RabbitMQ client library (works in Node/Bun contexts).
- `ajv` — JSON schema validation for incoming messages.
- `pino` — structured logging (optional; you can use console.log).

If you prefer an abstraction that manages reconnection for you, consider `amqp-connection-manager`. The examples below use `amqplib` so the logic is explicit.

---

File layout (suggested)

```ddia/rabbitmq/consumer/README.md#L53-80
.
├── package.json
├── src
│   ├── consumer.js        # main consumer implementation
│   ├── config.js          # configuration for broker, exchange, queue
│   ├── processor.js       # domain-specific message processing
│   └── validator.js       # JSON schema + AJV setup
├── docker-compose.yml
└── README.md
```

---

Example consumer implementation (detailed)

Below is a full, well-commented consumer example. Save it as `src/consumer.js`.

```ddia/rabbitmq/consumer/README.md#L81-320
// src/consumer.js
// Example RabbitMQ consumer using amqplib with manual acks, prefetch, validation,
// reconnect/backoff, and graceful shutdown.

import amqp from "amqplib";
import Ajv from "ajv";
import pino from "pino";

const log = pino({ level: process.env.LOG_LEVEL || "info" });

/**
 * Config - move these to `config.js` or environment variables in production.
 */
const RABBIT_URL = process.env.RABBITMQ_URL || "amqp://localhost:5672";
const EXCHANGE = process.env.EXCHANGE || "myapp.events";
const EXCHANGE_TYPE = "topic";
const QUEUE = process.env.QUEUE || "myapp.consumer.queue";
const ROUTING_KEY = process.env.ROUTING_KEY || "user.*";
const PREFETCH = Number(process.env.PREFETCH || 10);
const DLX = `${QUEUE}.dlx`;           // dead-letter exchange
const MAX_RECONNECT_MS = 30_000;

/**
 * Minimal AJV validator example. In a real project, import schemas from files.
 */
const ajv = new Ajv();
const messageSchema = {
  type: "object",
  required: ["id","type","timestamp","payload"],
  properties: {
    id: { type: "string" },
    type: { type: "string" },
    timestamp: { type: "string" },
    payload: { type: "object" },
    meta: { type: "object" }
  },
  additionalProperties: true
};
const validate = ajv.compile(messageSchema);

/**
 * Application message processor - replace with your domain logic.
 * Should be idempotent or handle duplicates.
 */
async function processMessage(message) {
  // Example: simulate processing
  log.info({ id: message.id, type: message.type }, "processing message");
  // Simulate occasional failure for demonstration
  if (message.type === "user.fail") {
    throw new Error("simulated processing error");
  }
  // Business logic here...
  return { ok: true };
}

/**
 * Create and set up channel and queue topology.
 */
async function setupTopology(channel) {
  // Declare main exchange and queue (durable)
  await channel.assertExchange(EXCHANGE, EXCHANGE_TYPE, { durable: true });
  await channel.assertExchange(DLX, "fanout", { durable: true });

  // Queue properties include dead-letter-exchange so failed messages go to DLX
  await channel.assertQueue(QUEUE, {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": DLX
    }
  });

  // Bind queue to routing key
  await channel.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY);

  // DLX queue for inspection
  await channel.assertQueue(`${QUEUE}.dlq`, { durable: true });
  await channel.bindQueue(`${QUEUE}.dlq`, DLX, "");
}

/**
 * Consume loop with manual ack/nack and JSON parsing + schema validation.
 */
async function startConsumer() {
  let conn;
  let channel;
  let shouldStop = false;

  // Reconnect logic with exponential backoff
  const connectWithBackoff = async () => {
    let attempt = 0;
    while (!shouldStop) {
      try {
        attempt++;
        log.info({ attempt }, "attempting to connect to RabbitMQ");
        conn = await amqp.connect(RABBIT_URL);
        conn.on("close", () => {
          if (!shouldStop) log.warn("connection closed, will attempt reconnect");
        });
        conn.on("error", (err) => {
          log.error({ err }, "connection error");
        });
        channel = await conn.createChannel();
        await channel.prefetch(PREFETCH);
        await setupTopology(channel);
        log.info("connected and topology declared");
        return { conn, channel };
      } catch (err) {
        const backoff = Math.min(1000 * Math.pow(2, attempt), MAX_RECONNECT_MS);
        log.error({ err, backoff }, "connect failed, retrying after backoff");
        await new Promise((res) => setTimeout(res, backoff));
      }
    }
    throw new Error("stopped reconnect attempts");
  };

  const { channel: ch } = await connectWithBackoff();

  // Consumer handler
  const onMessage = async (msg) => {
    if (!msg) {
      log.warn("received null message (consumer was canceled?)");
      return;
    }

    const content = msg.content.toString("utf8");
    let parsed;
    try {
      parsed = JSON.parse(content);
    } catch (err) {
      log.error({ err, content }, "invalid JSON - sending to DLX");
      // Invalid JSON - reject without requeue so it lands in DLX
      ch.nack(msg, false, false);
      return;
    }

    // Validate
    const valid = validate(parsed);
    if (!valid) {
      log.error({ errors: validate.errors, message: parsed }, "schema validation failed - sending to DLX");
      ch.nack(msg, false, false);
      return;
    }

    // Process
    try {
      await processMessage(parsed);
      ch.ack(msg);
    } catch (err) {
      log.error({ err, message: parsed }, "processing failed - sending to DLX");
      // Option A: send to DLX immediately
      ch.nack(msg, false, false);

      // Option B: implement retry with headers and a retry exchange (not shown here)
    }
  };

  // Start consuming
  await ch.consume(QUEUE, onMessage, { noAck: false });
  log.info("consumer started");

  // Graceful shutdown
  const shutdown = async () => {
    shouldStop = true;
    log.info("shutdown initiated");
    try {
      await ch.close();
      await conn.close();
      log.info("shutdown complete");
      process.exit(0);
    } catch (err) {
      log.error({ err }, "error during shutdown");
      process.exit(1);
    }
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

startConsumer().catch((err) => {
  console.error("consumer failed to start", err);
  process.exit(1);
});
```

Notes about the example:

- Manual acknowledgement gives you control to move invalid messages to a DLX.
- `prefetch` limits in-flight messages so the consumer doesn't get flooded.
- The consumer handles invalid JSON and validation errors and rejects them to a DLX.
- For retries (N attempts), implement a retry exchange/queue or use headers + delayed retry (via x-delayed-message plugin or separate queues with TTLs).

---

Reconnection & backoff

Important items:

- On transient network issues, the connection can drop. Use exponential backoff (1s, 2s, 4s, up to max).
- When `conn` emits `close` or `error`, attempt to reconnect. If using `amqp-connection-manager`, a lot of this is handled for you.
- Avoid busy-loop reconnections; always wait before retrying.

Example backoff snippet (conceptual):

```ddia/rabbitmq/consumer/README.md#L321-360
// Exponential backoff: delay = min(base * 2^attempt, max)
const backoff = Math.min(1000 * Math.pow(2, attempt), 30_000);
await sleep(backoff);
```

---

Dead-letter & retry strategy

Common strategies:

- Simple DLX: messages that fail validation or processing immediately go to DLX where they are inspected and reprocessed manually.
- Retry queues: route failed messages to `queue.retry.1` with TTL, then to `queue.retry.2` etc. After max attempts, route to final DLQ.
- Header-based retry: increment `attempt` in `meta` and re-publish with delay.

Pros/cons:

- DLX: easy, good for poison messages and debugging.
- Retry queues: automatic retry, but can complicate topology.
- Delayed plugin: simpler requeuing but requires RabbitMQ plugin.

Example queue arguments to enable DLX:

```ddia/rabbitmq/consumer/README.md#L361-384
await channel.assertQueue(QUEUE, {
  durable: true,
  arguments: {
    "x-dead-letter-exchange": DLX,
    // optional: "x-message-ttl": 60000, // TTL for retry queue
  }
});
```

---

Running locally (docker-compose)

Quick RabbitMQ for local dev:

```ddia/rabbitmq/consumer/README.md#L385-438
# docker-compose.yml
version: "3.8"
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
```

Start:

```ddia/rabbitmq/consumer/README.md#L439-446
docker-compose up -d
# then start the consumer
bun run src/consumer.js
```

(If your `package.json` has a `start` script, run `bun run start`.)

---

Observability & metrics

Track:

- Connection uptime / reconnect count.
- Messages consumed / processed / failed.
- Processing latency (histogram).
- DLQ counts.

Use structured logs (pino) and ideally push metrics to Prometheus via a small exporter or use StatsD.

---

Testing & troubleshooting

Testing:

- Unit test `processor.js` logic independently.
- Integration test against a local RabbitMQ using docker-compose.
- Use a test helper to publish messages and assert they are acked or landed in DLQ.

Troubleshooting:

- If messages disappear: check `ack/nack`, queue durability, and broker logs.
- If consumer doesn't get messages: verify bindings and routing keys.
- To inspect DLQ or queue contents, use the RabbitMQ Management UI at `http://localhost:15672` (default guest/guest).

---

Idempotency, ordering, and delivery guarantees

- This consumer example provides at-least-once delivery. Expect duplicates — make business logic idempotent using `id` deduplication or idempotency keys.
- Ordering is only guaranteed within a single consumer and single queue when prefetch=1. With multiple consumers or prefetch>1, ordering is not guaranteed.
- If strict ordering is required, consider partitioning messages by a key and routing to per-key queues/consumers.

---

Helpful tips & recommendations

- Use `confirm` channels for publishers but consumers just need regular channels.
- Set reasonable prefetch (e.g., 1-50) depending on processing time and memory.
- Prefer JSON schema validation to fail fast and avoid processing invalid payloads.
- Add health checks that assert RabbitMQ connectivity for orchestrators (Kubernetes).

---

Sample messages for manual testing

```ddia/rabbitmq/consumer/README.md#L447-476
# publish a test message (node)
import amqp from "amqplib";

(async () => {
  const conn = await amqp.connect("amqp://localhost");
  const ch = await conn.createChannel();
  const exchange = "myapp.events";
  await ch.assertExchange(exchange, "topic", { durable: true });

  const msg = {
    id: "test-1",
    type: "user.created",
    timestamp: new Date().toISOString(),
    payload: { userId: "u-123", name: "Alice" }
  };

  ch.publish(exchange, "user.created", Buffer.from(JSON.stringify(msg)), { persistent: true });
  await ch.close();
  await conn.close();
})();
```

---

Final notes

This README is a practical guide for building a robust RabbitMQ consumer. Customize the topology, retry strategy, and schema validation according to your domain needs. The code examples are intentionally explicit so you can adapt and extend them for production — swap in connection managers, metrics libraries, or advanced retry patterns as your requirements evolve.

If you want, I can:

- Provide a `package.json` and `src` scaffolding.
- Convert the example to TypeScript with typed schemas.
- Add integration test examples using `testcontainers` or Docker.
