import IORedis from 'ioredis';
import dotenv from 'dotenv';
dotenv.config();

// Creamos conexión con Redis usando URL de Upstash
const redis = new IORedis(process.env.REDIS_URL, {
  maxRetriesPerRequest: null,
  tls: {}, // Muy importante para conexiones "rediss://"
});

async function flushRedis() {
  try {
    console.log('⚠️ ADVERTENCIA: Vamos a eliminar TODOS los datos de Redis');
    console.log('Esperando 5 segundos antes de proceder...');
    
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    console.log('Ejecutando FLUSHALL...');
    const result = await redis.flushall();
    
    console.log(`Resultado: ${result}`);
    console.log('✅ Todas las colas y datos de Redis han sido eliminados');
    
  } catch (error) {
    console.error('❌ Error al limpiar Redis:', error);
  } finally {
    await redis.quit();
  }
}

flushRedis().catch(console.error); 