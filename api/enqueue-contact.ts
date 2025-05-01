import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getStreamName, publishToStream, scheduleDelayedMessage } from './queue.js';
import dotenv from 'dotenv';
import { Pool } from 'pg';
dotenv.config();

// Conexión a PostgreSQL usando DATABASE_URL en .env
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// Función para convertir locationId a BigInt
function locationIdToBigInt(locationId: string): bigint {
  let hash = 0n;
  for (let i = 0; i < locationId.length; i++) {
    hash = (hash * 31n + BigInt(locationId.charCodeAt(i))) & 0x7fffffffffffffffn;
  }
  return hash;
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Sólo POST, chavo.' });
  }

  try {
    console.log('🧠 Webhook recibido:\n', JSON.stringify(req.body, null, 2));

    const contactId = req.body.contact_id as string;
    const locationId = req.body?.location?.id as string;
    const workflowId = req.body?.workflow?.id as string || 'default';
    const timeframe = req.body?.customData?.TimeFrame as string;

    if (!contactId || !locationId || !timeframe) {
      return res.status(400).json({ error: 'Faltan datos (contactId, locationId o TimeFrame)' });
    }

    // Parsear "min to max"
    const match = timeframe.match(/(\d+(?:\.\d+)?)\s*to\s*(\d+(?:\.\d+)?)/i);
    if (!match) {
      return res.status(400).json({ error: 'TimeFrame mal formado, usa "60 to 300"' });
    }
    const min = parseFloat(match[1]);
    const max = parseFloat(match[2]);

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // 🔥 Lock para este locationId
      const lockId = locationIdToBigInt(locationId);
      await client.query('SELECT pg_advisory_xact_lock($1)', [lockId]);

      // 🔥 Buscar si ya tenemos el custom field id en la tabla
      let customFieldId: string | null = null;
      const fieldRes = await client.query(
        'SELECT timerdone_custom_field_id FROM location_custom_fields WHERE location_id = $1 LIMIT 1',
        [locationId]
      );

      if (fieldRes.rows.length > 0) {
        customFieldId = fieldRes.rows[0].timerdone_custom_field_id;
      }

      if (!customFieldId) {
        // Buscar en GHL
        const ghRes = await fetch(
          `https://gh-connector.vercel.app/proxy/locations/${locationId}/customFields`,
          {
            headers: {
              Authorization: process.env.GHL_API_KEY || '',
              LocationId: locationId,
            },
          }
        );
        const fieldsPayload = await ghRes.json();
        const allFields = Array.isArray(fieldsPayload)
          ? fieldsPayload
          : fieldsPayload.customFields;
        const timerField = allFields.find((f: any) => f.name?.toLowerCase() === 'timerdone');
        if (!timerField) {
          await client.query('ROLLBACK');
          return res.status(500).json({ error: 'Custom field "timerdone" no encontrado en GHL' });
        }

        customFieldId = timerField.id;

        await client.query(
          `INSERT INTO location_custom_fields (location_id, timerdone_custom_field_id, created_at)
           VALUES ($1, $2, NOW())
           ON CONFLICT (location_id) DO NOTHING`,
          [locationId, customFieldId]
        );
      }

      // 🔥 Leer el último run_at para este locationId Y workflowId
      let lastRunAt = new Date();
      const lastResult = await client.query(
        'SELECT run_at FROM sequential_queue WHERE location_id = $1 AND workflow_id = $2 ORDER BY run_at DESC LIMIT 1',
        [locationId, workflowId]
      );
      if (lastResult.rows.length > 0) {
        lastRunAt = new Date(lastResult.rows[0].run_at);
      }

      const now = new Date();
      if (lastRunAt < now) {
        lastRunAt = now;
      }

      // 🔥 Elegir un delay aleatorio
      const delaySeconds = Math.floor(Math.random() * (max - min + 1)) + Math.floor(min);
      const newRunAt = new Date(lastRunAt.getTime() + delaySeconds * 1000);

      // 🔥 Insertar en sequential_queue con workflowId
      await client.query(
        `INSERT INTO sequential_queue
          (contact_id, location_id, workflow_id, delay_seconds, custom_field_id, run_at)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [contactId, locationId, workflowId, delaySeconds, customFieldId, newRunAt]
      );

      await client.query('COMMIT');

      // 🔥 Obtener nombre del stream basado en locationId y workflowId
      const streamName = getStreamName(locationId, workflowId);
      
      // 🔥 Calcular delay
      const delayMs = Math.max(0, newRunAt.getTime() - now.getTime());
      console.log(`⏱️ Delay calculado: ${delayMs / 1000}s para ${streamName}`);

      // Datos a publicar en el stream
      const messageData = {
        contactId,
        locationId,
        workflowId,
        customFieldId,
        runAt: newRunAt.toISOString(),
        enqueuedAt: now.toISOString()
      };

      // Si hay delay, programar para más tarde
      if (delayMs > 0) {
        await scheduleDelayedMessage(streamName, messageData, delayMs);
        console.log(`📅 Contacto ${contactId} programado para ${newRunAt.toISOString()}`);
      } else {
        // Publicar inmediatamente en el stream
        const messageId = await publishToStream(streamName, messageData);
        console.log(`✅ Contacto ${contactId} publicado en stream ${streamName} con ID ${messageId}`);
      }

      return res.status(200).json({ 
        success: true,
        streamName,
        runAt: newRunAt.toISOString(),
        delayed: delayMs > 0
      });
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('🔥 ERROR ENCOLANDO:', error.stack || error.message || error);
      return res.status(500).json({ error: 'Error interno del servidor' });
    } finally {
      client.release();
    }
  } catch (err: any) {
    console.error('🔥 ERROR GENERAL:', err.stack || err.message || err);
    return res.status(500).json({ error: 'Error interno del servidor' });
  }
}
