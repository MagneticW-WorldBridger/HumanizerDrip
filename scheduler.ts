import { redisClient, publishToStream } from './api/queue.js';
import dotenv from 'dotenv';
dotenv.config();

/**
 * Procesa mensajes que fueron programados para ser publicados después
 * Busca claves con el patrón 'delayed:*' y, si ya pasó su tiempo de procesamiento,
 * los publica en su respectivo stream y elimina la clave de Redis
 */
async function processDelayedMessages() {
  console.log('⏱️ Iniciando procesador de mensajes retrasados...');
  
  while (true) {
    try {
      // Obtener todas las claves que coincidan con el patrón
      const now = Date.now();
      const keys = await redisClient.keys('delayed:*');
      
      if (keys.length > 0) {
        console.log(`🔍 Encontrados ${keys.length} mensajes retrasados`);
      }
      
      for (const key of keys) {
        // Formato de la clave: delayed:TIMESTAMP:streamName:contactId
        const parts = key.split(':');
        const timestamp = parseInt(parts[1]);
        
        // Si ya pasó el tiempo programado
        if (timestamp <= now) {
          // Obtener los datos del mensaje
          const dataString = await redisClient.get(key);
          if (dataString) {
            const { streamName, data } = JSON.parse(dataString);
            
            // Publicar en el stream correspondiente
            const messageId = await publishToStream(streamName, data);
            console.log(`🚀 Mensaje retrasado publicado en stream ${streamName} con ID ${messageId}`);
            
            // Eliminar la clave de Redis
            await redisClient.del(key);
          }
        }
      }
      
      // Esperar 1 segundo antes de la siguiente verificación
      await new Promise(resolve => setTimeout(resolve, 1000));
    } catch (error) {
      console.error('❌ Error procesando mensajes retrasados:', error);
      // Esperar 5 segundos antes de reintentar en caso de error
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
}

// Iniciar el procesador
processDelayedMessages().catch(error => {
  console.error('❌ Error fatal en scheduler:', error);
  process.exit(1);
});

// Manejar señales para una limpieza adecuada
process.on('SIGINT', () => {
  console.log('👋 Cerrando scheduler de mensajes retrasados...');
  redisClient.disconnect();
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('👋 Cerrando scheduler de mensajes retrasados...');
  redisClient.disconnect();
  process.exit(0);
}); 