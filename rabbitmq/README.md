# ddia/rabbitmq — Overview

This directory contains a minimal Producer and Consumer implementation that demonstrates patterns for publishing and consuming messages with RabbitMQ. The focus of these READMEs is code-first: what each component does, the message schema, recommended RabbitMQ topology (exchanges/queues/bindings), and robust patterns for reliability and observability.

Related READMEs

- `producer/README.md` — implementation and run details for the publisher
- `consumer/README.md` — implementation and run details for the consumer

Goals

- Show a simple, production-minded message flow.
- Demonstrate safe publish (persistent messages, confirms) and safe consume (manual acks, prefetch).
- Provide concrete JSON message schema and examples.
- Outline reconnection, idempotency, and error handling patterns.

High-level flow

1. Producer constructs a semantic event (JSON) and publishes to an exchange.
2. Exchange routes to one or more queues by routing key or bindings.
3. Consumer(s) pull messages from a queue, process them, then explicitly ack/nack/reject.
4. For failures, messages may be retried, moved to a dead-letter queue, or logged/alerted.

Topology recommendation

- Exchange: `events` (type: `topic`)
- Queues: `serviceA.worker`, `serviceB.worker`
- Dead-letter exchange: `events.dlx` with corresponding DLQ `serviceA.dlq`, etc.
- Routing keys: semantic event names like `user.created`, `order.paid`, `invoice.failed`

Why `topic` exchange?

- Flexible routing by patterns (e.g., `user.*`, `order.#`).
- Decouples producers from concrete consumer queue names.

Message format (recommended)
All messages are JSON with a small envelope containing metadata for routing/idempotency:

{
"id": "uuid-v4", // unique identifier for dedupe
"type": "user.created", // semantic event type / routing key
"source": "auth-service", // originating service
"timestamp": 1670000000000, // epoch ms
"payload": { ... } // event-specific data
}

Example:
{
"id": "b7f7c9f2-1c6b-4f39-9b0b-a0a1b3d2e5f6",
"type": "user.created",
"source": "signup-api",
"timestamp": 1680000000000,
"payload": {
"userId": "u_12345",
"email": "alice@example.com",
"createdAt": "2024-04-01T12:34:56Z"
}
}

Publishing best-practices

- Use persistent messages (deliveryMode = 2).
- Use publisher confirms (confirm channel) where available to know broker accepted message.
- Set `contentType: application/json` and `messageId`, `timestamp` AMQP properties.
- If ordering matters, avoid multiple competing publishers to the same partitioning key, or design accordingly.

Consuming best-practices

- Use manual ack (noAck: false) so you can ack only on successful processing.
- Set `prefetch` to limit in-flight messages per consumer (e.g., `prefetch(10)`).
- On processing errors:
  - Retry with exponential backoff (either application-level or by requeueing a few times).
  - Use a dead-letter exchange/queue for messages that exceed retry threshold.
- Ensure consumers are idempotent using the `id` field (dedupe by storing processed IDs or implementing idempotent operations).

Environment variables (suggested)

- RABBITMQ_URL — amqp URI, e.g. `amqp://guest:guest@localhost:5672/`
- RABBITMQ_EXCHANGE — `events`
- RABBITMQ_DLX — `events.dlx`
- RABBITMQ_PREFETCH — max in-flight messages (consumer)

Producer example (JavaScript / Node / Bun)
A minimal example showing confirms and persistent publish:

```/dev/null/producer_example.js#L1-200
// Example: publisher using amqplib (Node-compatible)
// NOTE: adapt import to your runtime (bun/node)
const amqp = require('amqplib');

async function publishEvent(rabbitUrl, exchange, routingKey, event) {
  const conn = await amqp.connect(rabbitUrl);
  try {
    // Use a confirm channel to get publisher confirms
    const ch = await conn.createConfirmChannel();
    await ch.assertExchange(exchange, 'topic', { durable: true });

    const payload = Buffer.from(JSON.stringify(event));
    const opts = {
      persistent: true,           // deliveryMode=2
      contentType: 'application/json',
      messageId: event.id,
      timestamp: Date.now()
    };

    await new Promise((resolve, reject) => {
      ch.publish(exchange, routingKey, payload, opts, (err, ok) => {
        if (err) return reject(err);
        resolve(ok);
      });
    });
    await ch.waitForConfirms(); // ensure broker persisted the message
    await ch.close();
  } finally {
    await conn.close();
  }
}

// Usage example:
const event = {
  id: 'uuid-v4',
  type: 'user.created',
  source: 'signup-api',
  timestamp: Date.now(),
  payload: { userId: 'u_12345', email: 'alice@example.com' }
};
publishEvent(process.env.RABBITMQ_URL || 'amqp://localhost', 'events', event.type, event)
  .then(() => console.log('published'))
  .catch(console.error);
```

Consumer example (JavaScript / Node / Bun)
Manual ack, prefetch, and simple DLX handling:

```/dev/null/consumer_example.js#L1-200
const amqp = require('amqplib');

async function runConsumer(rabbitUrl, queue) {
  const conn = await amqp.connect(rabbitUrl);
  const ch = await conn.createChannel();
  const prefetch = parseInt(process.env.RABBITMQ_PREFETCH || '10', 10);
  await ch.prefetch(prefetch);

  // Ensure queue is durable and bound to exchange elsewhere
  ch.assertQueue(queue, { durable: true });

  ch.consume(queue, async (msg) => {
    if (!msg) return;
    try {
      const content = JSON.parse(msg.content.toString());
      // idempotency guard: skip if already processed (implement store check)
      // processMessage(content) -> your business logic
      await processMessage(content);
      ch.ack(msg);
    } catch (err) {
      // On transient errors, you may requeue. To avoid tight loops:
      // - implement retry count using headers or track in DB
      // - or reject and route to DLQ after max attempts
      const retryCount = (msg.properties.headers['x-retry'] || 0) + 1;
      if (retryCount <= 3) {
        // republish with incremented header and small delay (or use delayed exchange)
        ch.nack(msg, false, true); // requeue
      } else {
        // move to DLQ by rejecting without requeue (if DLX configured)
        ch.reject(msg, false);
      }
    }
  }, { noAck: false });
}

// stub:
async function processMessage(msg) {
  // Business logic here
  console.log('processing', msg.type, msg.id);
}

runConsumer(process.env.RABBITMQ_URL || 'amqp://localhost', 'serviceA.worker')
  .catch(console.error);
```

Retries and DLQ patterns

- RabbitMQ does not provide automatic delayed retries built-in in vanilla installs; options:
  - Use a separate delayed exchange plugin or use per-queue TTL + DLX to implement delayed retry.
  - Implement application-level retry counts and republish with a "retry" header and a delay queue.
- Dead-lettering: configure the consumer queue with `x-dead-letter-exchange: events.dlx` so rejected messages route to DLQ.

Observability

- Add correlation IDs in headers to trace across systems.
- Publish events to an audit/logging queue (or log to a central store) for replay/debug.
- Export metrics: in-flight messages, queue depth, publish confirms, consumer errors.

Security

- Use AMQPS when crossing untrusted networks (TLS).
- Limit user permissions (grant only what is necessary).
- Rotate credentials and use secrets manager for connection strings.

Operational tips

- Monitor queue length and set alarms when depths grow beyond expected thresholds.
- Tune `prefetch` by measuring throughput and latency; fewer messages per consumer reduces memory usage and improves fairness.
- If you need strict ordering, dedicate a single consumer per ordering key.

Folder layout (suggested)

- `producer/` — publisher code, tests, Dockerfile if applicable
- `consumer/` — consumer code, tests, Dockerfile if applicable
- `infrastructure/` — Terraform/K8s manifests for exchanges/queues, policies (optional)

FAQ
Q: Should I use `amqplib` or another client?
A: `amqplib` is a widely used Node client. Pick the client compatible with your runtime (Bun/Node/Deno). Use a client that supports confirm channels and manual acks.

Q: How to ensure exactly-once processing?
A: Exactly-once is hard across distributed systems. Aim for at-least-once delivery with idempotent consumers. Use deduplication stores or idempotent writes on the consumer-side.

Q: What about message schemas and evolution?
A: Keep event `type` in the envelope and version payloads. Consumers should tolerate extra fields and use versioned processing if necessary.

Next steps

- Review `producer/README.md` and `consumer/README.md` for runtime and install steps.
- Add infrastructure manifests to ensure the recommended exchanges and DLX are created with the expected arguments.
- Implement idempotency store (Redis, DB) for consumers that require exactly-once semantics.

If you want, I can:

- Provide concrete Dockerfile / compose files to run RabbitMQ, producer, and consumer locally.
- Write example infra (Terraform/K8s) to create exchanges/queues with DLX and bindings.
