import type { VercelRequest, VercelResponse } from '@vercel/node';
import { contactQueue } from './queue.js';
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

      // 🔥 Leer el último run_at para este locationId
      let lastRunAt = new Date();
      const lastResult = await client.query(
        'SELECT run_at FROM sequential_queue WHERE location_id = $1 ORDER BY run_at DESC LIMIT 1',
        [locationId]
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

      // 🔥 Insertar en sequential_queue
      await client.query(
        `INSERT INTO sequential_queue
          (contact_id, location_id, delay_seconds, custom_field_id, run_at)
         VALUES ($1, $2, $3, $4, $5)`,
        [contactId, locationId, delaySeconds, customFieldId, newRunAt]
      );

      await client.query('COMMIT');

      // 🔥 Encolar en BullMQ
      const delayMs = newRunAt.getTime() - now.getTime();
      console.log(`⏱️ Delay calculado: ${delayMs / 1000}s`);

      await contactQueue.add(
        'ghl-contact',
        {
          contactId,
          locationId,
          customFieldId,
        },
        {
          delay: delayMs,
          jobId: `${contactId}-${Date.now()}`,
        }
      );

      console.log(`✅ Contacto ${contactId} encolado con ${delayMs / 1000}s`);
      return res.status(200).json({ success: true });
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
