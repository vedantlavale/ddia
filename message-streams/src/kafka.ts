import { Kafka } from "kafkajs"

export const kafka = new Kafka({
  clientId: "bun-app",
  brokers: ["localhost:9092"],
});
