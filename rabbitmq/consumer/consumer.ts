import amqp from "amqplib";

const queue = "tasks";
const url = process.env.RABBITMQ_URL || "amqp://localhost";

async function connectWithRetry() {
  while (true) {
    try {
      return await amqp.connect(url);
    } catch {
      console.log("Retrying connection...");
      await new Promise(res => setTimeout(res, 2000));
    }
  }
}

async function consume() {
  const conn = await connectWithRetry();
  const channel = await conn.createChannel();

  await channel.assertQueue(queue, { durable: true });

  channel.prefetch(1);

  console.log("Waiting for messages...");

  channel.consume(queue, (msg) => {
    if (msg) {
      const content = msg.content.toString();
      console.log("Received:", content);

      setTimeout(() => {
        console.log("Processed:", content);
        channel.ack(msg);
      }, 2000);
    }
  });
}

consume();
