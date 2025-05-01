import IORedis from 'ioredis';
import dotenv from 'dotenv';
dotenv.config();

// Creamos conexión con Redis usando URL de Upstash
const redis = new IORedis(process.env.REDIS_URL, {
  maxRetriesPerRequest: null,
  tls: {}, // Muy importante para conexiones "rediss://"
});

async function checkRedis() {
  try {
    console.log('Comprobando conexión a Redis...');
    
    // Buscar todas las claves Bull
    const bullKeys = await redis.keys('bull:*');
    console.log(`Encontradas ${bullKeys.length} claves de Bull en Redis`);
    
    if (bullKeys.length > 0) {
      console.log('Primeras 10 claves:');
      for (let i = 0; i < Math.min(10, bullKeys.length); i++) {
        console.log(`- ${bullKeys[i]}`);
      }
    }
    
    // Buscar colas delayed específicamente
    const delayedQueues = await redis.keys('bull:*:delayed');
    console.log(`\nColas delayed: ${delayedQueues.length}`);
    
    for (const delayedQueue of delayedQueues) {
      const count = await redis.zcard(delayedQueue);
      console.log(`- ${delayedQueue}: ${count} trabajos`);
      
      if (count > 0) {
        const jobs = await redis.zrange(delayedQueue, 0, 2, 'WITHSCORES');
        for (let i = 0; i < jobs.length; i += 2) {
          const jobId = jobs[i];
          const score = jobs[i + 1];
          const date = new Date(parseInt(score) / 0x1000);
          console.log(`  • Job ${jobId}: ${date.toISOString()} (${score})`);
        }
      }
    }
    
    // Buscar colas activas
    const activeQueues = await redis.keys('bull:*:active');
    console.log(`\nColas activas: ${activeQueues.length}`);
    
    // Buscar colas waiting
    const waitingQueues = await redis.keys('bull:*:wait');
    console.log(`\nColas en espera: ${waitingQueues.length}`);
    
    for (const waitQueue of waitingQueues) {
      const count = await redis.llen(waitQueue);
      console.log(`- ${waitQueue}: ${count} trabajos`);
    }
    
  } catch (error) {
    console.error('Error al consultar Redis:', error);
  } finally {
    await redis.quit();
  }
}

checkRedis().catch(console.error); 