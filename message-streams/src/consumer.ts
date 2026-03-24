import { kafka } from "./kafka";

const consumer = kafka.consumer({ groupId: "Bottles group" })
await consumer.connect()
await consumer.subscribe({ topic: "bottles", fromBeginning: true })
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({
      partition,
      offset: message.offset,
      value: message.value?.toString(),
    });
  },
})
