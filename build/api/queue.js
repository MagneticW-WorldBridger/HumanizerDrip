import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import dotenv from 'dotenv';
dotenv.config();
// Nos conectamos al Redis de Upstash vía TLS (rediss://...)
const redis = new IORedis(process.env.REDIS_URL, {
    maxRetriesPerRequest: null
});
export const contactQueue = new Queue('contactos', {
    connection: redis,
    // opcional: attempts y backoff
    defaultJobOptions: {
        attempts: 3,
        backoff: { type: 'exponential', delay: 60000 }
    }
});
