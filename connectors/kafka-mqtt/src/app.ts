import dotenv from 'dotenv';
import mqtt from 'mqtt';
import { Kafka } from 'kafkajs';
dotenv.config();

const { MQTT_URL, MQTT_PORT, MQTT_USER, MQTT_PASS, KAFKA_BROKERS, KAFKA_TOPICS } = process.env;
const mqttClient  = mqtt.connect(MQTT_URL, {
	clientId: 'MQTT Client',
	host: MQTT_URL,
	port: parseInt(MQTT_PORT),
	username: MQTT_USER,
	password: MQTT_PASS,
	clean: true,
	protocol: 'ssl',
});

// Mqtt error calback
mqttClient.on('error', (err) => {
	console.log(err);
	mqttClient.end();
});

// Connection callback
mqttClient.on('connect', () => {
	console.log('mqtt client connected');
});
mqttClient.subscribe('bess/#', { qos: 0 });

const kafka = new Kafka({
	clientId: 'kafka-mongo-connector',
	brokers: KAFKA_BROKERS.split(',')
});
const consumer = kafka.consumer({ groupId: 'kafka-mqtt-connector' });
const publishMqtt = async (bess, topic, message) => {
	try{
		const response = await mqttClient.publish(`bess/${bess}/${topic}`, message);
		console.log(response);
	}
	catch(error){
		console.error(error);
	}
};
const run = async () => {
	try {
		await consumer.connect();
		KAFKA_TOPICS.split(',').map(async(topic) => {
			await consumer.subscribe({ topic, fromBeginning: true });
		});

		await consumer.run({
			eachMessage: async ({ topic, message }) => {
				const data = JSON.parse(message.value.toString());
				const topicMqtt = topic.split('_').join('/');
				publishMqtt(message.key.toString(), topicMqtt, JSON.stringify({
					Data: data,
					Timestamp: new Date().toISOString()
				}));
			},
		});

	}
	catch(error){
		console.error(error);
	}
};
run();