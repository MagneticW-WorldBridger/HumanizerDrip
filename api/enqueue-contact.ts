import type { VercelRequest, VercelResponse } from '@vercel/node';
import { contactQueue } from './queue.js';
import dotenv from 'dotenv';
import { Pool } from 'pg';
dotenv.config();

// Conexión a PostgreSQL
const pool = new Pool({
  connectionString: process.env.POSTGRES_URL,
  ssl: { rejectUnauthorized: false },
});

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Sólo POST, chavo.' });
  }

  try {
    console.log('🧠 Webhook recibido:\n', JSON.stringify(req.body, null, 2));

    const contactId = req.body.contact_id;
    const locationId = req.body?.location?.id;
    const timeframe = req.body?.customData?.TimeFrame;

    if (!contactId || !locationId || !timeframe) {
      return res.status(400).json({ error: 'Faltan datos (contactId, locationId o TimeFrame)' });
    }

    const match = timeframe.match(/(\d+(?:\.\d+)?)\s*to\s*(\d+(?:\.\d+)?)/i);
    if (!match) {
      return res.status(400).json({ error: 'TimeFrame mal formado, usa "60 to 300"' });
    }
    const min = parseFloat(match[1]);
    const max = parseFloat(match[2]);

    // Consultar el último run_at para este locationId
    const client = await pool.connect();
    let lastRunAt: Date = new Date();
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

    // Calcular el nuevo run_at
    const delaySeconds = Math.floor(Math.random() * (max - min + 1)) + min;
    const newRunAt = new Date(lastRunAt.getTime() + delaySeconds * 1000);

    // Insertar el nuevo run_at en la tabla
    const client2 = await pool.connect();
    try {
      await client2.query(
        'INSERT INTO sequential_queue (location_id, run_at) VALUES ($1, $2)',
        [locationId, newRunAt]
      );
    } finally {
      client2.release();
    }

    // Calcular el delay en milisegundos desde ahora
    const delayMs = newRunAt.getTime() - Date.now();
    console.log(`⏱️ Delay calculado: ${delayMs / 1000}s`);

    // Buscar el custom field "timerdone" en GHL
    const fieldRes = await fetch(`https://gh-connector.vercel.app/proxy/locations/${locationId}/customFields`, {
      headers: {
        Authorization: process.env.GHL_API_KEY || '',
        LocationId: locationId,
      },
    });
    const fieldsArr = await fieldRes.json();
    const allFields = Array.isArray(fieldsArr) ? fieldsArr : fieldsArr.customFields;
    const timerField = allFields.find((f: any) => f.name?.toLowerCase() === 'timerdone');

    if (!timerField) {
      return res.status(500).json({ error: 'Custom field "timerdone" no encontrado en GHL' });
    }

    // Encolar el trabajo con el delay calculado
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