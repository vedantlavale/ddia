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

async function send() {
  const conn = await connectWithRetry();
  const channel = await conn.createChannel();

  await channel.assertQueue(queue, { durable: true });

  let count = 1;

  setInterval(() => {
    const msg = `Task ${count++}`;
    channel.sendToQueue(queue, Buffer.from(msg), { persistent: true });
    console.log("Sent:", msg);
  }, 2000);
}

send();
