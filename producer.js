const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'weather-sensor',
  brokers: ['kafka:9092']
});

const producer = kafka.producer();
const topic = 'weather-data';

const generateWeatherData = () => {
  const stationId = `STATION-0${Math.floor(Math.random() * 3) + 1}`;
  return {
    stationId,
    timestamp: new Date().toISOString(),
    temperature: (Math.random() * 35).toFixed(1),   // 0 a 35 °C
    humidity: (Math.random() * 50 + 50).toFixed(1), // 50 a 100 %
    windSpeed: (Math.random() * 30).toFixed(1)      // 0 a 30 km/h
  };
};

const run = async () => {
  await producer.connect();
  console.log("🌡️ Sensor del clima conectado y enviando datos...");

  setInterval(async () => {
    try {
      const weatherData = generateWeatherData();
      await producer.send({
        topic,
        messages: [{ value: JSON.stringify(weatherData) }],
      });
      console.log(`[Sensor] Datos enviados para ${weatherData.stationId}`);
    } catch (error) {
      console.error("[Sensor] Error al enviar datos:", error);
    }
  }, 3000); // Enviar cada 3 segundos
};

run().catch(e => console.error('[Sensor] Error:', e));