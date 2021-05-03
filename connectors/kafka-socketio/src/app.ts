import dotenv from 'dotenv';
import { Kafka } from 'kafkajs';
import { createServer } from 'http';
import { Server } from 'socket.io';
import express from 'express';
dotenv.config();

const { KAFKA_BROKERS, KAFKA_TOPICS, APP_URL, PORT } = process.env;

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.all('/*', function (req, res, next) {
	var oneof = false;
	if (req.headers.origin) {
		res.header('Access-Control-Allow-Origin', req.headers.origin);
		oneof = true;
	}
	if (req.headers['access-control-request-method']) {
		res.header(
			'Access-Control-Allow-Methods',
			req.headers['access-control-request-method']
		);
		oneof = true;
	}
	if (req.headers['access-control-request-headers']) {
		res.header(
			'Access-Control-Allow-Headers',
			req.headers['access-control-request-headers']
		);
		oneof = true;
	}
	if (oneof) {
		res.header('Access-Control-Max-Age', `${60 * 60 * 24 * 365}`);
	}

	// intercept OPTIONS method
	if (oneof && req.method == 'OPTIONS') {
		res.sendStatus(200);
	} else {
		next();
	}
});
app.get('/', (_, res)=> {
	res.status(418).send({
		message: 'Welcome to Socket.io Service.'
	});
});
app.set('port', PORT);

const httpServer = createServer(app);
// eslint-disable-next-line no-undef
const socketIOLocals = new WeakMap();
const io = new Server(httpServer, {
	transports: ['websocket', 'polling'],
	cors: {
		origin: [`${APP_URL}`, '*'], //"https://app.dev.bess.laceti.com.br"
		methods: ['GET', 'POST'], 
	}
});
const checkToken = (authHeader: any) => {
	// TODO send auth header to auth service to check
	console.log(authHeader);
	return true;
};
const kafka = new Kafka({
	clientId: 'my-app',
	brokers: KAFKA_BROKERS.split(',')
});
const consumer = kafka.consumer({ groupId: 'kafka-socket-connector' });
io.use((socket, next) => {
	let query = socket.handshake.query;
	const {token} = query;
	if ((typeof query === 'object') && (typeof token === 'string')){
		let decoded = checkToken(token);
		socketIOLocals.set(socket, decoded);
		next();
	}
	else {
		next(new Error('Authentication error'));
	} 
});
const emitSocket = (message: {value: any, key: any}, topic: string ) => {
	const data = JSON.parse(message.value.toString()).Data;
	const tzCloud = JSON.parse(message.value.toString()).TimestampCloud;
	const tz = `${JSON.parse(message.value.toString()).Timestamp}`.length === 19 ? 
		`${JSON.parse(message.value.toString()).Timestamp}.000Z` : 
		JSON.parse(message.value.toString()).Timestamp;
	io.to(message.key.toString()).emit(topic, {
		...data,
		bess: message.key.toString(),
		createdAt: tzCloud ? tzCloud : tz,
		updatedAt: tzCloud ? tzCloud : tz
	});
};
const main = async () => {
	try{
		await consumer.connect();
		KAFKA_TOPICS.split(',').map(async(topic) => {
			await consumer.subscribe({ topic, fromBeginning: true });
		});

		await consumer.run({
			eachMessage: async ({ topic, message }) => {
				emitSocket(message, topic);
			},
		});
	}
	catch(error){
		console.error(error);
	}
};
main().then(() => console.log('done'));