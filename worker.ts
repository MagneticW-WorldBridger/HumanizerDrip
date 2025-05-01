import { redisClient, createConsumerGroup, getStreamName } from './api/queue.js';
import { Pool } from 'pg';
import dotenv from 'dotenv';
import * as crypto from 'crypto';
dotenv.config();

// Generamos un ID único para este worker
const WORKER_ID = process.env.WORKER_ID || `worker-${crypto.randomBytes(4).toString('hex')}`;
console.log(`🆔 Worker ID: ${WORKER_ID}`);

// Nombre del grupo de consumidores (compartido por todos los workers)
const CONSUMER_GROUP = 'ghl-drip-workers';

// Conexión a PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// Mapa para rastrear qué streams estamos procesando
const activeStreams = new Map<string, boolean>();

// Función para llamar a GHL y actualizar contacto
async function updateContact(contactId: string, locationId: string, customFieldId: string) {
  console.log(`🔔 Actualizando contacto ${contactId}`);
  const res = await fetch(
    `https://gh-connector.vercel.app/proxy/contacts/${contactId}`,
    {
      method: 'PUT',
      headers: {
        'Authorization': process.env.GHL_API_KEY || '',
        'LocationId': locationId,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        customFields: [{ id: customFieldId, field_value: 'YES' }]
      })
    }
  );
  
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`❌ Falló GHL (${res.status}): ${body}`);
  }
  
  console.log(`✅ Contacto ${contactId} actualizado`);
}

// Función para borrar el contacto de la tabla de PostgreSQL
async function removeFromQueue(contactId: string, locationId: string, workflowId: string = 'noworkflow') {
  const client = await pool.connect();
  try {
    await client.query(
      `DELETE FROM sequential_queue 
       WHERE contact_id = $1 AND location_id = $2 AND workflow_id = $3`,
      [contactId, locationId, workflowId || 'noworkflow']
    );
    console.log(`🗑️ Contacto ${contactId} borrado de sequential_queue`);
  } finally {
    client.release();
  }
}

// Función para procesar un mensaje de un stream
async function processStreamMessage(
  streamName: string, 
  messageId: string, 
  messageData: Record<string, string>
) {
  try {
    console.log(`⚙️ Procesando mensaje ${messageId} del stream ${streamName}`);
    
    // Extraer datos del mensaje
    const contactId = messageData.contactId;
    const locationId = messageData.locationId;
    const workflowId = messageData.workflowId || 'noworkflow';
    const customFieldId = messageData.customFieldId;
    
    // Actualizar contacto en GHL
    await updateContact(contactId, locationId, customFieldId);
    
    // Eliminar de la tabla en PostgreSQL
    await removeFromQueue(contactId, locationId, workflowId);
    
    // Confirmar que el mensaje ha sido procesado
    await redisClient.xack(streamName, CONSUMER_GROUP, messageId);
    
    // Opcionalmente, eliminar el mensaje del stream
    // Esto es útil para mantener el stream pequeño, pero pierdes el historial
    if (process.env.DELETE_PROCESSED_MESSAGES === 'true') {
      await redisClient.xdel(streamName, messageId);
    }
    
    console.log(`✅ Mensaje ${messageId} procesado correctamente`);
    return true;
  } catch (error) {
    console.error(`❌ Error procesando mensaje ${messageId}:`, error);
    // No hacemos XACK para que otro worker pueda intentarlo después
    return false;
  }
}

// Función para procesar un stream específico
async function processStream(streamName: string) {
  if (activeStreams.has(streamName)) {
    return; // Ya estamos procesando este stream
  }
  
  console.log(`🎯 Iniciando procesamiento del stream: ${streamName}`);
  activeStreams.set(streamName, true);
  
  try {
    // Crear grupo de consumidores si no existe
    await createConsumerGroup(streamName, CONSUMER_GROUP);
    
    // Bucle principal para procesar mensajes
    while (activeStreams.get(streamName)) {
      try {
        // Leer mensajes nuevos o pendientes de este stream
        // '>' significa "dame mensajes nuevos que nadie ha visto"
        const streamMessages = await redisClient.xreadgroup(
          'GROUP', CONSUMER_GROUP, WORKER_ID, 
          'COUNT', 1, // Procesar de uno en uno para garantizar orden
          'BLOCK', 2000, // Bloquear 2 segundos, luego comprobar otros streams
          'STREAMS', streamName, '>'
        );
        
        // Si hay mensajes nuevos
        if (streamMessages && streamMessages.length > 0) {
          const [stream] = streamMessages;
          const [_, messages] = stream as [string, any[]];
          
          if (messages.length > 0) {
            const [messageId, fields] = messages[0] as [string, string[]];
            
            // Convertir el array de campos a un objeto
            const messageData: Record<string, string> = {};
            for (let i = 0; i < fields.length; i += 2) {
              messageData[fields[i]] = fields[i + 1];
            }
            
            await processStreamMessage(streamName, messageId, messageData);
          }
        } else {
          // Si no hay mensajes nuevos, verificar mensajes pendientes
          try {
            // Intentar obtener información de mensajes pendientes
            const pendingResult = await redisClient.xpending(
              streamName, CONSUMER_GROUP, '-', '+', 1
            );
            
            // XPENDING devuelve diferentes estructuras según la versión de Redis
            // Intentar manejar diferentes formatos posibles
            if (pendingResult) {
              let pendingMessageId = '';
              
              // Verificar si es un arreglo con mensajes pendientes
              if (Array.isArray(pendingResult) && pendingResult.length > 0) {
                // Formato Redis >= 6.2: Array de entradas pendientes
                const firstPending = pendingResult[0];
                
                // El ID podría estar en diferentes posiciones según formato
                if (typeof firstPending === 'string') {
                  pendingMessageId = firstPending; // ID directo
                } else if (Array.isArray(firstPending) && firstPending.length > 0) {
                  pendingMessageId = firstPending[0]; // [ID, ...otros datos]
                } else if (typeof firstPending === 'object' && firstPending !== null) {
                  // Formato de objeto { id: string, ... }
                  pendingMessageId = (firstPending as any).id || '';
                }
              } else if (
                typeof pendingResult === 'object' && 
                pendingResult !== null && 
                'count' in pendingResult && 
                (pendingResult as any).count > 0
              ) {
                // Formato antiguo: objeto con información agregada
                // No podemos obtener IDs directamente, ignoramos
              }
              
              // Si tenemos un ID pendiente, intentar reclamarlo
              if (pendingMessageId) {
                try {
                  const claimed = await redisClient.xclaim(
                    streamName, 
                    CONSUMER_GROUP, 
                    WORKER_ID, 
                    30000, 
                    pendingMessageId
                  );
                  
                  if (claimed && Array.isArray(claimed) && claimed.length > 0) {
                    const [claimedData] = claimed;
                    if (Array.isArray(claimedData) && claimedData.length >= 2) {
                      const [claimedId, fields] = claimedData as [string, string[]];
                      
                      // Convertir el array de campos a un objeto
                      const messageData: Record<string, string> = {};
                      for (let i = 0; i < fields.length; i += 2) {
                        messageData[fields[i]] = fields[i + 1];
                      }
                      
                      await processStreamMessage(streamName, claimedId, messageData);
                    }
                  }
                } catch (claimError) {
                  console.error(`❌ Error al reclamar mensaje pendiente:`, claimError);
                }
              }
            }
          } catch (pendingError) {
            console.error(`❌ Error al verificar mensajes pendientes:`, pendingError);
          }
        }
      } catch (error) {
        console.error(`❌ Error leyendo del stream ${streamName}:`, error);
        // Esperar un segundo antes de reintentar
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  } catch (error) {
    console.error(`❌ Error procesando stream ${streamName}:`, error);
  } finally {
    activeStreams.delete(streamName);
  }
}

// Función para descubrir streams existentes
async function discoverStreams() {
  try {
    console.log('🔍 Buscando streams existentes...');
    
    // Método 1: Buscar streams en Redis mediante patrón
    const streamKeys = await redisClient.keys('stream:location:*');
    
    // Para cada stream encontrado, iniciar procesamiento
    for (const streamKey of streamKeys) {
      if (!activeStreams.has(streamKey)) {
        processStream(streamKey).catch(console.error);
      }
    }
    
    // Método 2: Buscar combinaciones location+workflow en PostgreSQL
    const client = await pool.connect();
    try {
      // Buscar entradas recientes (últimas 24 horas)
      const result = await client.query(
        `SELECT DISTINCT location_id, workflow_id 
         FROM sequential_queue 
         WHERE run_at > NOW() - INTERVAL '24 hours'`
      );
      
      // Para cada combinación, crear stream si no existe y procesar
      for (const row of result.rows) {
        const streamName = getStreamName(row.location_id, row.workflow_id);
        if (!activeStreams.has(streamName)) {
          processStream(streamName).catch(console.error);
        }
      }
    } finally {
      client.release();
    }
    
    console.log(`🔄 Procesando ${activeStreams.size} streams actualmente`);
  } catch (error) {
    console.error('❌ Error descubriendo streams:', error);
  }
  
  // Ejecutar cada 30 segundos
  setTimeout(discoverStreams, 30000);
}

// Iniciar descubrimiento de streams
discoverStreams().catch(error => {
  console.error('❌ Error fatal en worker:', error);
  process.exit(1);
});

// Manejar señales para una limpieza adecuada
process.on('SIGINT', () => {
  console.log('👋 Cerrando worker...');
  redisClient.disconnect();
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('👋 Cerrando worker...');
  redisClient.disconnect();
  process.exit(0);
});