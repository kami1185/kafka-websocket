const { Kafka } = require('kafkajs');

const kafka = new Kafka({ clientId: 'alerts-processor', brokers: ['kafka:9092'] });
const consumer = kafka.consumer({ groupId: 'alerts-processor-group' });
const producer = kafka.producer(); // Para producir las alertas

const run = async () => {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: 'weather-data', fromBeginning: true });
  console.log("Procesador de Alertas suscrito a 'weather-data'.");

  await consumer.run({
    eachMessage: async ({ message }) => {
      const weatherData = JSON.parse(message.value.toString());

      if (weatherData.temperature > 25.0) {
        const alert = {
          type: 'ALERTA_TEMPERATURA',
          message: `¡Temperatura Extrema! ${weatherData.temperature}°C en ${weatherData.stationId}`,
          timestamp: new Date().toISOString()
        };
        
        await producer.send({
          topic: 'weather-alerts',
          messages: [{ value: JSON.stringify(alert) }]
        });
        console.log(`Alerta de temperatura generada para ${weatherData.stationId}`);
      }
    },
  });
};

run().catch(console.error);