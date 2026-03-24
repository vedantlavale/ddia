import { kafka } from "./kafka";

const producer = kafka.producer();
await producer.connect();

await producer.send({
  topic: "bottles",
  messages: [
    { key: "bottle-1", value: JSON.stringify({ id: 1, item: "milton", qty: 2 }) },
    { key: "bottle-2", value: JSON.stringify({ id: 2, item: "coca-cola",  qty: 5 }) },
  ],
});

await producer.disconnect();
