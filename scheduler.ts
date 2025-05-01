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
            workflowId: row.workflow_id || 'noworkflow',
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

// Main scheduler loop: process ready contacts from Postgres based on run_at
async function runSchedulerLoop() {
  console.log('⏱️ Scheduler loop started...');
  while (true) {
    try {
      await processReadyContacts();
    } catch (error) {
      console.error('❌ Error in scheduler loop:', error);
    }
    // Wait 1 second before next iteration
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}

// Start the scheduler loop
runSchedulerLoop().catch(error => {
  console.error('❌ Fatal error in scheduler:', error);
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