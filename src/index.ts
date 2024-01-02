import { getMessageFromKafka } from "./configurations/kafka";
import { client } from "./configurations/redis";


getMessageFromKafka("missileDataPSI-1") 
client.connect()