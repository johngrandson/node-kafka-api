import express from 'express';

import KafkaConnection from '../../utils/kafkaConn';

const app = express();

const kafka = KafkaConnection.connect('certificate');

const topic = 'issue-certificate'

const consumer = kafka.consumer({ groupId: 'certificate-group' })
const producer = kafka.producer();

/**
 * Disponibiliza o producer para todas rotas
 */
app.use((req, res, next) => {
  req.producer = producer;

  return next();
})

async function run() {
  await consumer.connect()
  await consumer.subscribe({ topic })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `Tópico: ${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)

      const payload = JSON.parse(message.value);

      producer.send({
        topic: 'certification-response',
        messages: [
          { value: `Certificado do usuário ${payload.user.name} do curso ${payload.course} gerado!` }
        ]
      })

      producer.send({
        topic: 'create-user',
        messages: [
          { value: `Usuário ${payload.user.name} com email ${payload.user.email} gerado!` }
        ]
      })
    },
  })
}

run().catch(console.error)