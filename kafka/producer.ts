import { kafka } from "./kafka";

const producer = kafka.producer()
await producer.connect()
await producer.send({
  topic: 'orders',
  messages: [
    { key: 'thingone', value: JSON.stringify({ id: 1, item: 'thingone', quantity: 1 }) },
    { key: 'thingtwo', value: JSON.stringify({ id: 2, item: 'thingtwo', quantity: 2 }) },
    { key: 'thingthree', value: JSON.stringify({ id: 3, item: 'thingthree', quantity: 3 }) },
  ]
})
