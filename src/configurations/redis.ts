import { createClient } from "redis";
import dotenv from "dotenv";


dotenv.config();
const REDIS_HOST = process.env.REDIS_HOST || ""
const REDIS_PASSWORD = process.env.REDIS_PASSWORD || ""
const REDIS_PORT = process.env.REDIS_PORT || ""



export const client = createClient({
    password: REDIS_PASSWORD,
    socket: {
      host: REDIS_HOST,
      port: Number(REDIS_PORT),
    },
  });
  