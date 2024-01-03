import { Kafka, logLevel } from 'kafkajs';
import dotenv from "dotenv";
import { client } from './redis';
import { Data } from './types';

dotenv.config();

const KAFKA_HOST = process.env.KAFKA_HOST || ""
const KAFKA_USERNAME = process.env.KAFKA_USERNAME || ""
const KAFKA_PASS = process.env.KAFKA_PASS || ""



export const kafka = new Kafka({
    clientId: 'my-app',
    brokers: [KAFKA_HOST],
    ssl: true,
    sasl: {
        mechanism: 'scram-sha-256',
        username: KAFKA_USERNAME,
        password: KAFKA_PASS
    },
    logLevel: logLevel.ERROR,
});


export const consumer = kafka.consumer({ groupId: "my-group" });


export const getMessageFromKafka = async (topic: string) => {
    try {
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: true });
        await consumer.run({
            eachMessage: async ({ message, topic }) => {
                try {
                    const messageString = message.value && message.value.toString('utf-8');
                    const kafkaMessage = messageString && JSON.parse(messageString)
                    kafkaMessage.time = Number(message.key)
                    const oldData = await client.exists(`countryes-test3:${kafkaMessage.source}:${kafkaMessage.destination}`);
                    if (oldData) {
                        console.log(`countryes-test2:${kafkaMessage.source}:${kafkaMessage.destination}`);
                        await client.json.numIncrBy(`countryes-test3:${kafkaMessage.source}:${kafkaMessage.destination}`, 'rounds', 1)
                        await client.json.numIncrBy(`countryes-test3:${kafkaMessage.source}:${kafkaMessage.destination}`, 'missileAmount', kafkaMessage.missileAmount)
                        await client.json.set(`countryes-test2:${kafkaMessage.source}:${kafkaMessage.destination}`, 'lastUpdateTime', kafkaMessage.time)
                    }
                    else {
                        client.json.set(`countryes-test3:${kafkaMessage.source}:${kafkaMessage.destination}`, `$`, {
                            country: kafkaMessage.source,
                            area: kafkaMessage.destination,
                            rounds: 1,
                            missileAmount: kafkaMessage.missileAmount,
                            creationTime: kafkaMessage.time,
                            lastUpdateTime: kafkaMessage.time
                        })
                    }
                } catch (error) {
                    console.error("Error processing Kafka message:", error);
                }
            }
        });
    } catch (error) {
        if (error instanceof Error) return Promise.reject(error);
    }
};


