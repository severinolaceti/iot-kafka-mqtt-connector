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
	clientId: 'my-app',
	brokers: KAFKA_BROKERS.split(',')
});
const producer = kafka.producer();

const publishKafka = async (topic, message, key) => {
	await producer.connect();
	const response = await producer.send({
		topic,
		messages: [
			{ value: message, key },
		],
	});
	if(response[0].errorCode === 0){
		console.log(new Date().toISOString(),key, topic);
	}
	await producer.disconnect();
};
const main = async () => {
	try{
		const admin = kafka.admin();
		await admin.connect();
		const topics = KAFKA_TOPICS.split(',').map(topic => {
			return {
				topic,
				numPartitions: 2,
				replicationFactor: 1
			};
		});
		await admin.createTopics({
			topics,
		});
		mqttClient.on('message', async (topicName, message) => {
			// message is Buffer
			const [, key, topic] = topicName.split('/');
			await publishKafka(topic, message.toString(), key);
		});
		mqttClient.on('close', () => {
			console.log('MQTT client disconnected');
		});
	}
	catch(error){
		console.error(error);
	}
};
main().then(() => console.log('done'));