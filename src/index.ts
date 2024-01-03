import { getMessageFromKafka } from "./configurations/kafka";
import { connectToRedis } from "./configurations/redis";


getMessageFromKafka("missileDataPSI-1") 
connectToRedis()