import { kafka } from '../kafka'
const email = kafka.consumer({ groupId: 'email-group' })

await email.connect()
await email.subscribe({ topic: 'orders', fromBeginning: true })

await email.run({
  eachMessage: async ({  message }) => {
    console.log(`Sending email for order ${message.key}`)
  },
})
