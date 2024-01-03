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
                    const dateObj = new Date(Number(message.key));
                    kafkaMessage.time = Number(message.key)
                    const key = kafkaMessage.source + "-" + kafkaMessage.destination
                    const oldData = await client.json.get(key) as unknown as Data
                    console.log(kafkaMessage);
                    // const currentValue = await client.json.get('countryes', `$.${key}`);
                    await client.json.set('countryes', '$', {
                        [key]: {
                            rounds: 1,
                            missileAmount: kafkaMessage.missileAmount,
                            creationTime: kafkaMessage.time,
                            lastUpdateTime: kafkaMessage.time
                        }
                    });
                    // if (!oldData) {
                    //     await client.json.set(key, '$', {
                    //         rounds: 1,
                    //         missileAmount: kafkaMessage.missileAmount,
                    //         creationTime: kafkaMessage.time,
                    //         lastUpdateTime: kafkaMessage.time
                    //     });
                    // }
                    // else {
                    //     await client.json.set(key, '$', {
                    //         rounds: oldData.rounds + 1,
                    //         missileAmount: oldData.missileAmount + kafkaMessage.missileAmount,
                    //         creationTime: oldData.creationTime,
                    //         lastUpdateTime: kafkaMessage.time
                    //     })
                    // }
                } catch (error) {
                    console.error("Error processing Kafka message:", error);
                }
            }
        });
    } catch (error) {
        if (error instanceof Error) return Promise.reject(error);
    }
};


