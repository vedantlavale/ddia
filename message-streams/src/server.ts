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

console.log("Server running on http://localhost:3000");
