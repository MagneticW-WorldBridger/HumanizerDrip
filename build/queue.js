import { Queue } from 'bullmq';
export const contactQueue = new Queue('contactos', {
    connection: {
        host: process.env.UPSTASH_REDIS_REST_URL,
        password: process.env.UPSTASH_REDIS_REST_TOKEN,
        tls: {}
    }
});
