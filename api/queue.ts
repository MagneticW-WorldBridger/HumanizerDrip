import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import dotenv from 'dotenv';
dotenv.config();

// Creamos conexión con Redis usando URL de Upstash
const redis = new IORedis(process.env.REDIS_URL!, {
  maxRetriesPerRequest: null,
  tls: {}, // Muy importante para conexiones "rediss://"
});

export const contactQueue = new Queue('contactos', {
  connection: redis,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 60000 // 1 minuto entre reintentos si falla
    }
  }
});