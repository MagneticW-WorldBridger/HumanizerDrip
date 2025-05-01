# Implementación de Redis Streams para GHL Drip Server

## Objetivos del Sistema

1. **Procesamiento Secuencial Por Ubicación y Workflow:**
   - Cada combinación de ubicación+workflow tiene su propia cola secuencial
   - Las diferentes ubicaciones o diferentes workflows deben procesarse en paralelo

2. **Reglas de Organización de Colas:**
   - ✅ Misma ubicación + mismo workflow = UNA cola secuencial (para mantener el orden)
   - ✅ Misma ubicación + diferentes workflows = colas PARALELAS
   - ✅ Diferentes ubicaciones = colas PARALELAS

## Implementación con Redis Streams (COMPLETADA)

### Archivos Creados/Modificados

1. **api/queue.ts** ✅ 
   - Reemplazamos BullMQ con funciones de utilidad para Redis Streams
   - Implementamos `getStreamName()` para crear nombres de stream dinámicos
   - Agregamos funciones para publicar y programar mensajes

2. **api/enqueue-contact.ts** ✅
   - Modificado para usar Redis Streams
   - Añadido soporte para identificar y separar por workflowId
   - Cada combinación locationId+workflowId tendrá su propio stream

3. **scheduler.ts** ✅
   - Nuevo servicio para procesar mensajes retrasados
   - Busca mensajes cuyo tiempo de procesamiento ha llegado
   - Los publica en sus respectivos streams

4. **worker.ts** ✅
   - Reemplazado el worker de BullMQ por consumer de Redis Streams
   - Implementa descubrimiento dinámico de streams
   - Procesa cada stream en paralelo, pero secuencialmente dentro de cada stream
   - Usa grupos de consumidores para garantizar procesamiento confiable

5. **update-schema.sql** ✅
   - Script para actualizar la estructura de la base de datos
   - Agrega columna `workflow_id` a la tabla `sequential_queue`
   - Crea índice para mejorar rendimiento

### Beneficios de la Solución

- **Mayor paralelismo:** Cada combinación locationId+workflowId se procesa independientemente
- **Mejor performance:** Uso directo de Redis Streams es más eficiente que BullMQ
- **Escalabilidad:** Podemos correr múltiples workers que colaboran automáticamente
- **Tolerancia a fallos:** Los mensajes no procesados se recuperan automáticamente
- **Secuencialidad garantizada:** Cada stream se procesa en orden estricto

## Pasos para Desplegar

1. **Actualizar la base de datos**
   ```bash
   psql $DATABASE_URL -f update-schema.sql
   ```

2. **Limpiar Redis**
   ```bash
   node flush-redis.js
   ```

3. **Desplegar a Vercel**
   ```bash
   vercel --prod
   ```

4. **Iniciar servicios en Railway**
   - Servicio 1: Worker principal (worker.ts)
   - Servicio 2: Scheduler para mensajes retrasados (scheduler.ts)

## Verificación y Monitoreo

Para verificar que el sistema funciona correctamente:

1. **Enviar webhook de prueba a la API:**
   ```bash
   curl -X POST https://ghl-drip-server.vercel.app/api/enqueue-contact \
     -H "Content-Type: application/json" \
     -d '{"contact_id":"test123","location":{"id":"456"},"workflow":{"id":"humanizer"},"customData":{"TimeFrame":"60 to 300"}}'
   ```

2. **Verificar streams en Redis:**
   ```
   redis-cli -u $REDIS_URL --tls
   > KEYS stream:*
   > XINFO STREAM stream:location:456:workflow:humanizer
   ```

3. **Consultar contactos en PostgreSQL:**
   ```sql
   SELECT * FROM sequential_queue ORDER BY run_at DESC LIMIT 10;
   ```

## Mantenimiento y Escalado

- Para agregar más capacidad de procesamiento, simplemente inicia más instancias del worker
- Cada worker descubrirá automáticamente los streams y se coordinará con los demás
- Si un worker se cae, los mensajes pendientes serán procesados por otros workers 