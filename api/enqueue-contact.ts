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

    // 1) Obtener el custom field "timerdone"
    const fieldRes = await fetch(
      `https://gh-connector.vercel.app/proxy/locations/${locationId}/customFields`,
      {
        headers: {
          Authorization: process.env.GHL_API_KEY || '',
          LocationId: locationId,
        },
      }
    );
    const fieldsPayload = await fieldRes.json();
    const allFields = Array.isArray(fieldsPayload)
      ? fieldsPayload
      : fieldsPayload.customFields;
    const timerField = allFields.find(
      (f: any) => f.name?.toLowerCase() === 'timerdone'
    );
    if (!timerField) {
      return res
        .status(500)
        .json({ error: 'Custom field "timerdone" no encontrado en GHL' });
    }

    // 2) Leer el último run_at para este locationId
    const client = await pool.connect();
    let lastRunAt = new Date(); // si la tabla está vacía, arranca desde ahora
    try {
      const result = await client.query(
        'SELECT run_at FROM sequential_queue WHERE location_id = $1 ORDER BY run_at DESC LIMIT 1',
        [locationId]
      );
      if (result.rows.length > 0) {
        lastRunAt = new Date(result.rows[0].run_at);
      }
    } finally {
      client.release();
    }

    // 3) Asegurarnos de no programar en el pasado
    const now = new Date();
    if (lastRunAt < now) {
      lastRunAt = now;
    }

    // 4) Elegir un delay aleatorio dentro del timeframe
    const delaySeconds =
      Math.floor(Math.random() * (max - min + 1)) + Math.floor(min);
    const newRunAt = new Date(lastRunAt.getTime() + delaySeconds * 1000);

    // 5) Insertar en la tabla todos los campos obligatorios
    const client2 = await pool.connect();
    try {
      await client2.query(
        `INSERT INTO sequential_queue
          (contact_id, location_id, delay_seconds, custom_field_id, run_at)
         VALUES ($1, $2, $3, $4, $5)`,
        [contactId, locationId, delaySeconds, timerField.id, newRunAt]
      );
    } finally {
      client2.release();
    }

    // 6) Calcular el delay real desde ahora
    const delayMs = newRunAt.getTime() - now.getTime();
    console.log(`⏱️ Delay calculado: ${delayMs / 1000}s`);

    // 7) Encolar en BullMQ con ese delay
    await contactQueue.add(
      'ghl-contact',
      {
        contactId,
        locationId,
        customFieldId: timerField.id,
      },
      {
        delay: delayMs,
        jobId: `${contactId}-${Date.now()}`,
      }
    );

    console.log(`✅ Contacto ${contactId} encolado con ${delayMs / 1000}s`);
    return res.status(200).json({ success: true });
  } catch (err: any) {
    console.error('🔥 ERROR ENCOLANDO:', err.stack || err.message || err);
    return res.status(500).json({ error: 'Error interno del servidor' });
  }
}