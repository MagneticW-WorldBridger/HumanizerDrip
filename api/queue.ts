import { Queue } from 'bullmq';
import dotenv from 'dotenv';

// 2️⃣ Cargamos las llaves secretas del archivo .env
dotenv.config();

// 3️⃣ Creamos la cola “contactos” y le decimos dónde vive Redis (Upstash)
export const contactQueue = new Queue('contactos', {
  connection: {
    host: process.env.UPSTASH_REDIS_REST_URL!,   // URL que te dio Upstash
    password: process.env.UPSTASH_REDIS_REST_TOKEN!, // Token de Upstash
    tls: {}                                      // Conexión cifrada
  }
});
