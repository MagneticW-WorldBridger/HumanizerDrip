import IORedis from 'ioredis';
import dotenv from 'dotenv';
dotenv.config();

// Creamos conexión con Redis usando URL de Upstash
const redis = new IORedis(process.env.REDIS_URL!, {
  maxRetriesPerRequest: null,
  tls: {}, // Muy importante para conexiones "rediss://"
});

// Exportamos la conexión a Redis para usarla en otros módulos
export const redisClient = redis;

/**
 * Genera un nombre de stream basado en locationId y workflowId
 * @param locationId ID de la ubicación
 * @param workflowId ID del workflow (opcional)
 * @returns Nombre del stream
 */
export function getStreamName(locationId: string, workflowId: string = 'noworkflow'): string {
  return `stream:location:${locationId}:workflow:${workflowId}`;
}

/**
 * Publica un mensaje en un stream de Redis
 * @param streamName Nombre del stream
 * @param data Datos a publicar
 * @returns ID del mensaje publicado
 */
export async function publishToStream(streamName: string, data: Record<string, any>): Promise<string> {
  // Convertir el objeto a un array plano para xadd
  const entries = Object.entries(data).flatMap(([key, value]) => [key, typeof value === 'object' ? JSON.stringify(value) : value]);
  
  // Publicar al stream con ID automático '*'
  return redis.xadd(streamName, '*', ...entries);
}

/**
 * Crea un grupo de consumidores para un stream si no existe
 * @param streamName Nombre del stream
 * @param groupName Nombre del grupo
 * @returns true si el grupo se creó correctamente o ya existía, false en caso de error
 */
export async function createConsumerGroup(streamName: string, groupName: string): Promise<boolean> {
  try {
    // Crear grupo y stream si no existen (MKSTREAM es crucial)
    await redis.xgroup('CREATE', streamName, groupName, '0', 'MKSTREAM');
    console.log(`Grupo ${groupName} creado para stream ${streamName}`);
    return true; // Creado exitosamente
  } catch (error: any) {
    // Ignorar error si el grupo ya existe
    if (error.message.includes('BUSYGROUP')) {
      return true; // Ya existía, todo bien
    }
    console.error(`Error creando grupo para ${streamName}:`, error.message);
    return false; // Error no esperado
  }
}

/**
 * Guarda un mensaje para ser procesado después (cuando expire el delay)
 * @param streamName Nombre del stream donde se publicará eventualmente
 * @param data Datos del mensaje
 * @param delayMs Tiempo de espera en milisegundos
 */
export async function scheduleDelayedMessage(
  streamName: string,
  data: Record<string, any>,
  delayMs: number
): Promise<void> {
  const processAt = Date.now() + delayMs;
  const delayedKey = `delayed:${processAt}:${streamName}:${data.contactId || Date.now()}`;
  
  // Guardar datos con tiempo de expiración (10 segundos extra por seguridad)
  await redis.set(
    delayedKey,
    JSON.stringify({
      streamName,
      data,
      processAt
    }),
    'PX',
    delayMs + 10000
  );
  
  console.log(`Mensaje programado para ${new Date(processAt).toISOString()} en ${streamName}`);
}

// Limpieza para evitar memory leaks al salir
process.on('exit', () => {
  redis.disconnect();
});
