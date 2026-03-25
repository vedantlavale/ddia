import { kafka } from "../kafka";

const inventory = kafka.consumer({ groupId: 'inventory' })
await inventory.connect();
await inventory.subscribe({ topic: 'orders', fromBeginning: true })

await inventory.run({
  eachMessage: async ({ message }) => {
    console.log(`Deducting the stock for order ${message.key}`)
  },
})
