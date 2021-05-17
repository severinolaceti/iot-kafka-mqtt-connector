import dotenv from 'dotenv';
import { Kafka } from 'kafkajs';
import {MongoClient} from 'mongodb';
dotenv.config();

const { KAFKA_BROKERS, KAFKA_TOPICS, MONGO_URL, DB_NAME } = process.env;
const dbClient = new MongoClient(MONGO_URL, {
	useNewUrlParser: true,
	useUnifiedTopology: true,
});

dbClient.connect(async (err) => {
	if (err) throw err;
	console.log('MongoDB has been connected');
});
const kafka = new Kafka({
	clientId: 'kafka-mongo-connector',
	brokers: KAFKA_BROKERS.split(',')
});

const consumer = kafka.consumer({ groupId: 'kafka-mongo-connector' });

const subscribeKafkaTopics = async () => {
  const admin = kafka.admin ();
  let kafka_topics = await admin.listTopics();
    kafka_topics.map((topic) => {
      consumer.subscribe({ topic, fromBeginning: true });
    }); 
};

const run = async () => {
	try {
		await consumer.connect();
		// KAFKA_TOPICS.split(',').map(async(topic) => {
		// 	await consumer.subscribe({ topic, fromBeginning: true });
		// });

    subscribeKafkaTopics();
    
		await consumer.run({
			eachMessage: async ({ topic, partition, message }) => {
				const data = JSON.parse(message.value.toString()).Data;
				const tzCloud = JSON.parse(message.value.toString()).TimestampCloud;
				const tz = `${JSON.parse(message.value.toString()).Timestamp}`.length === 19 ? 
					`${JSON.parse(message.value.toString()).Timestamp}.000Z` : 
					JSON.parse(message.value.toString()).Timestamp;

				const result = await dbClient
					.db(DB_NAME)
					.collection(topic)
					.insertOne({
						...data,
						bess: message.key.toString(),
						createdAt: tzCloud ? tzCloud : tz,
						updatedAt: tzCloud ? tzCloud : tz
					});
				console.log(result.insertedCount);
				if(result.insertedCount === 1){
					console.log(new Date().toISOString(), partition, message.key.toString(), topic);
				}
				else{
					console.error(new Date().toISOString(), partition, message.key.toString(), topic);
				}
        subscribeKafkaTopics();
			},
		});

	}
	catch(error){
		console.error(error);
	}
};
run();