# RabbitMQ Producer — ddia/rabbitmq/producer

This repository contains a focused RabbitMQ _producer_ implementation and patterns used to publish messages from a service into RabbitMQ. The README below is intentionally code-focused: it explains how we connect, how we publish (including publisher confirms), reconnection/backoff, message contract, and a few realistic examples you can copy into your project.

Highlights

- Simple example to get started quickly
- Robust example with reconnection, publisher confirms, batching
- Message contract (JSON schema) and routing guidance
- How to run using Bun (project was created with `bun init`)

Table of contents

- Overview
- Architecture & message flow
- Message contract
- Dependencies
- Environment variables
- Quickstart (minimal producer)
- Robust producer (reconnect + confirms + backoff)
- Advanced notes: batching, TTL, dedup, idempotency
- Testing & local RabbitMQ
- Troubleshooting

---

## Overview

This producer publishes domain events as JSON messages to RabbitMQ. We favor:

- Explicit message schema with `type` and `meta`
- Publisher confirms for reliability
- Reconnection/backoff and safe shutdown
- Routing with exchanges (topic or direct) and routing keys

Typical usage:

1. Application constructs a message object (JSON).
2. Producer serializes JSON and publishes to an exchange with a routing key.
3. RabbitMQ routes message to queues based on bindings.
4. Consumers pick up the message and process it.

---

## Architecture & message flow

- Producer -> Exchange (topic/direct) -> Queue(s) -> Consumer(s)
- Producers are asynchronous and should handle connection loss gracefully.
- Use publisher confirms when the message must be durable and you need acknowledgment from broker.

We use a single responsibility `Producer` class that:

- Manages the connection and channel lifecycle
- Exposes `publish(exchange, routingKey, message, options)` returning a Promise that resolves when the broker confirms the message (if using confirm channel)
- Implements exponential backoff + jitter on connection failures

---

## Message contract

We recommend a small, explicit envelope:

- `type` (string): event type, e.g. `user.created`
- `id` (string): unique message id (UUID v4)
- `timestamp` (ISO string)
- `payload` (object): event-specific data
- `meta` (object, optional): source, correlation ids, version

Example schema (informal):

```/dev/null/message-schema.json#L1-20
{
  "type": "user.created",
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "timestamp": "2026-03-21T12:34:56.789Z",
  "payload": {
    "userId": "u-123",
    "email": "alice@example.com"
  },
  "meta": {
    "source": "auth-service",
    "correlationId": "req-456"
  }
}
```

Routing key convention (suggested):

- `service.event` or `entity.action` e.g. `user.created`, `order.updated`.

---

## Dependencies

This README uses the canonical Node-style RabbitMQ client `amqplib`. The project uses Bun, but `amqplib` works with Bun in many cases. If you use a native Bun library or other RabbitMQ clients, the patterns remain the same.

Install:

```bash
bun install amqplib uuid
# or (npm)
# npm install amqplib uuid
```

---

## Environment variables

Use these env vars to configure the producer:

- `RABBITMQ_URL` — e.g. `amqp://guest:guest@localhost:5672`
- `RABBITMQ_EXCHANGE` — default exchange name
- `RABBITMQ_EXCHANGE_TYPE` — `topic` or `direct` (default `topic`)
- `RABBITMQ_DURABLE` — whether exchanges/queues are durable (default `true`)

---

## Quickstart — Minimal producer

A minimal producer that connects and publishes one message.

```/Users/vedantlavale/Developer/ddia/rabbitmq/producer/producer.minimal.js#L1-120
import amqplib from "amqplib";
import { v4 as uuidv4 } from "uuid";

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://localhost:5672";
const EXCHANGE = process.env.RABBITMQ_EXCHANGE || "ddia.events";

async function run() {
  const conn = await amqplib.connect(RABBITMQ_URL);
  const ch = await conn.createChannel();

  await ch.assertExchange(EXCHANGE, "topic", { durable: true });

  const message = {
    type: "user.created",
    id: uuidv4(),
    timestamp: new Date().toISOString(),
    payload: { userId: "u-123", email: "alice@example.com" },
    meta: { source: "auth-service" }
  };

  const routingKey = "user.created";
  const body = Buffer.from(JSON.stringify(message));

  ch.publish(EXCHANGE, routingKey, body, { persistent: true });
  console.log("Published", message.id);

  await ch.close();
  await conn.close();
}

run().catch(err => {
  console.error("Publish failed", err);
  process.exit(1);
});
```

Notes:

- This uses a regular channel and does not wait for confirms: fast but less safe.
- Use `{ persistent: true }` to request RabbitMQ to persist message to disk (queue + exchange durable).

---

## Robust producer — Confirm channel + reconnect + backoff

Below is a more production-ready example. It:

- Uses a confirm channel (so we wait for broker ack)
- Reconnects on failure with exponential backoff + jitter
- Provides `publish` which returns a Promise resolved on confirm or rejected on error

```/Users/vedantlavale/Developer/ddia/rabbitmq/producer/producer.js#L1-400
import amqplib from "amqplib";
import { v4 as uuidv4 } from "uuid";

const DEFAULTS = {
  url: process.env.RABBITMQ_URL || "amqp://localhost:5672",
  exchange: process.env.RABBITMQ_EXCHANGE || "ddia.events",
  exchangeType: process.env.RABBITMQ_EXCHANGE_TYPE || "topic",
  durable: process.env.RABBITMQ_DURABLE !== "false"
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function backoff(attempt, base = 100, cap = 10000) {
  // exponential backoff with jitter
  const exp = Math.min(cap, base * Math.pow(2, attempt));
  const jitter = Math.random() * base;
  return Math.floor(exp + jitter);
}

export class Producer {
  constructor(opts = {}) {
    this.opts = { ...DEFAULTS, ...opts };
    this.conn = null;
    this.ch = null; // confirm channel
    this.connected = false;
    this._closing = false;
    this._connectAttempt = 0;
  }

  async connect() {
    if (this.connected) return;
    while (!this._closing) {
      try {
        this.conn = await amqplib.connect(this.opts.url);
        this.conn.on("error", err => {
          console.error("Connection error", err);
          this._handleDisconnect(err);
        });
        this.conn.on("close", () => {
          console.warn("Connection closed");
          this._handleDisconnect(new Error("Connection closed"));
        });

        this.ch = await this.conn.createConfirmChannel(); // confirm channel
        await this.ch.assertExchange(this.opts.exchange, this.opts.exchangeType, { durable: this.opts.durable });
        this.connected = true;
        this._connectAttempt = 0;
        console.log("Connected to RabbitMQ");
        return;
      } catch (err) {
        this._connectAttempt++;
        const wait = backoff(this._connectAttempt);
        console.error(`Connection attempt ${this._connectAttempt} failed. Retrying in ${wait}ms`, err);
        await sleep(wait);
      }
    }
  }

  _handleDisconnect(err) {
    this.connected = false;
    try { this.ch?.close().catch(()=>{}); } catch(e){}
    try { this.conn?.close().catch(()=>{}); } catch(e){}
    this.ch = null;
    this.conn = null;
    // reconnect in background
    this.connect().catch(e => console.error("Reconnect failed", e));
  }

  async publish(routingKey, message, opts = {}) {
    if (this._closing) throw new Error("Producer is closing");
    if (!this.connected) {
      await this.connect();
    }

    const envelope = {
      type: message.type || "unknown",
      id: message.id || uuidv4(),
      timestamp: message.timestamp || new Date().toISOString(),
      payload: message.payload || message,
      meta: message.meta || {}
    };

    const body = Buffer.from(JSON.stringify(envelope));
    const publishOpts = { persistent: true, contentType: "application/json", ...opts };

    return new Promise((resolve, reject) => {
      // Using confirm channel; callback is invoked when broker acks/nacks
      this.ch.publish(this.opts.exchange, routingKey, body, publishOpts, (err, ok) => {
        if (err) {
          return reject(err);
        }
        // ok is usually true when acked
        resolve({ ok, id: envelope.id });
      });
      // NOTE: If connection drops before ack, the callback may be invoked with an error.
    });
  }

  async close() {
    this._closing = true;
    this.connected = false;
    try { await this.ch?.close(); } catch (e) {}
    try { await this.conn?.close(); } catch (e) {}
  }
}

// Example usage (run directly)
if (require.main === module) {
  (async () => {
    const p = new Producer();
    await p.connect();

    try {
      const res = await p.publish("user.created", {
        payload: { userId: "u-123", email: "bob@example.com" },
        meta: { source: "signup-service" },
        type: "user.created"
      });
      console.log("Published and confirmed:", res);
    } catch (err) {
      console.error("Publish failed", err);
    } finally {
      await p.close();
    }
  })();
}
```

Why use confirm channels?

- With a normal channel, `ch.publish` returns true/false for write-buffer backpressure but does not guarantee the broker received or persisted the message.
- Confirm channels invoke a callback when the broker ack/nack the message. This avoids message loss when a broker fails after receiving but before persisting the message.

---

## Advanced patterns

Batching:

- Group multiple messages and publish sequentially in the same confirm channel, waiting for confirms for each, or use `waitForConfirms()` if your client exposes it. Batching increases throughput.

Idempotency & dedup:

- Include `id` in the envelope and ensure the consumer is idempotent. Optionally use a dedup store.

TTL and dead-lettering:

- Use message TTL and dead-letter exchanges/queues for messages that shouldn't live forever:
  - `ch.assertQueue(queueName, { deadLetterExchange: "dlx", messageTtl: 60000 })`

Persistent vs transient:

- `persistent: true` requests the broker to persist the message to disk. It is respected only if the queue is durable.

Transactions:

- Avoid AMQP transactions for high throughput; use confirms instead.

Correlation and request/response:

- For RPC-style interactions, set `correlationId` and `replyTo` properties. But prefer async messaging where possible.

---

## Running locally (RabbitMQ)

Run RabbitMQ locally (Docker):

```bash
docker run -it --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
# Management UI: http://localhost:15672 (guest/guest)
```

Run producer (example):

```bash
bun run producer/producer.js
# or
node producer/producer.js
```

(You may need to add scripts to `package.json` / `bunfig.toml` to run easily.)

---

## Testing

- Unit tests: mock `amqplib.connect` and assert `publish` called with correct exchange and routing key.
- Integration tests: use a test RabbitMQ instance (Docker) and assert messages are routed to a test queue. Clean up after tests.
- For end-to-end tests, publish a message and have a test consumer assert the payload and headers.

---

## Troubleshooting

- "Connection refused" — verify `RABBITMQ_URL`, ensure RabbitMQ is running.
- "ECONNRESET" or dropped connection — check network; the producer reconnect code should handle it.
- Publishes not seen by consumers:
  - Ensure exchange type and bindings match.
  - Ensure queue bindings exist and queues are durable/created before publishing if needed.
- Confirms never arrive:
  - Ensure you use a confirm channel (`createConfirmChannel`) and pass a callback to `publish`, or use `waitForConfirms`.

---

## FAQ

Q: Why not use transactions?
A: AMQP transactions are blocking and reduce throughput significantly. Publisher confirms are the recommended approach for reliability.

Q: Do I need durable queues?
A: If you require persistence across broker restarts, use durable queues and persistent messages.

Q: What about ordering?
A: RabbitMQ preserves ordering per-queue (FIFO), but routing to multiple queues or multiple consumers can change ordering semantics.

---

## Summary

This README focused on code-level patterns for creating a reliable RabbitMQ producer:

- Minimal and robust code examples provided
- Use publisher confirms for reliability
- Handle reconnects with exponential backoff
- Provide a clear message envelope and routing strategy

Copy the `Producer` class in `producer.js` into your service, wire the environment variables, and adapt the `publish` calls around your business logic.

If you want, I can add:

- A companion `consumer` README with sample consumer code and patterns
- TypeScript types and a JSON Schema for message validation
- Docker compose to run both producer and consumer for local testing
