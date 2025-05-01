import { redisClient, publishToStream, getStreamName } from './api/queue.js';
import { Pool } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

// Conexión a PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

/**
 * Busca contactos en PostgreSQL cuyo tiempo de ejecución ya ha llegado
 * y los publica en el stream correspondiente
 */
async function processReadyContacts() {
  const client = await pool.connect();
  try {
    // Buscar contactos cuyo run_at ya ha pasado
    const result = await client.query(
      `SELECT id, contact_id, location_id, workflow_id, custom_field_id, run_at
       FROM sequential_queue
       WHERE run_at <= NOW()
       LIMIT 50`  // Procesar en lotes para evitar sobrecarga
    );
    
    if (result.rows.length > 0) {
      console.log(`🔄 Procesando ${result.rows.length} contactos listos desde PostgreSQL`);
      
      for (const row of result.rows) {
        try {
          // Obtener nombre del stream para este contacto
          const streamName = getStreamName(row.location_id, row.workflow_id);
          
          // Datos a publicar en el stream
          const messageData = {
            contactId: row.contact_id,
            locationId: row.location_id,
            workflowId: row.workflow_id || 'default',
            customFieldId: row.custom_field_id,
            runAt: row.run_at.toISOString(),
            enqueuedAt: new Date().toISOString()
          };
          
          // Publicar en el stream
          const messageId = await publishToStream(streamName, messageData);
          console.log(`🚀 Contacto ${row.contact_id} publicado en stream ${streamName} con ID ${messageId}`);
          
          // Eliminar contacto de PostgreSQL
          await client.query('DELETE FROM sequential_queue WHERE id = $1', [row.id]);
          console.log(`🗑️ Contacto ${row.contact_id} eliminado de sequential_queue`);
        } catch (error) {
          console.error(`❌ Error procesando contacto ${row.contact_id}:`, error);
          // Continuar con el siguiente contacto
        }
      }
    }
  } catch (error) {
    console.error('❌ Error buscando contactos listos:', error);
  } finally {
    client.release();
  }
}

/**
 * Procesa mensajes que fueron programados para ser publicados después
 * Busca claves con el patrón 'delayed:*' y, si ya pasó su tiempo de procesamiento,
 * los publica en su respectivo stream y elimina la clave de Redis
 */
async function processDelayedMessages() {
  console.log('⏱️ Iniciando procesador de mensajes retrasados...');
  
  while (true) {
    try {
      // 1. Primero procesamos contactos listos en PostgreSQL
      await processReadyContacts();
      
      // 2. Luego procesamos mensajes delayed en Redis
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