import {Kafka} from 'kafkajs'


export const kafka = new Kafka({
  clientId: "demoapp",
  brokers:["localhost:9092"]

})
