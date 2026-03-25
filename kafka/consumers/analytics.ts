import { kafka } from "../kafka";
const analytics = kafka.consumer({ groupId: 'analytics' })
await analytics.connect();
await analytics.subscribe({ topic: 'orders', fromBeginning: true })

await analytics.run({
  eachMessage: async ({ message }) => {
    console.log(`Analyzing order ${message.key}`)
  },
})
