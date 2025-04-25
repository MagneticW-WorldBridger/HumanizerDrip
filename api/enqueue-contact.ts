import type { VercelRequest, VercelResponse } from '@vercel/node';
import { contactQueue } from './queue.js';          // ← viene de tu paso 3.2
import dotenv from 'dotenv';
dotenv.config();

// 2️⃣ Función que se ejecuta cuando GHL manda el webhook
export default async function handler(req: VercelRequest, res: VercelResponse) {
  // Solo aceptamos POST
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Sólo POST, chavo.' });
  }

  try {
    console.log('🧠 Webhook recibido:\n', JSON.stringify(req.body, null, 2));

    // 3️⃣ Sacamos los datos que necesitamos del cuerpo
    const contactId   = req.body.contact_id;
    const locationId  = req.body?.location?.id;
    const timeframe   = req.body?.customData?.TimeFrame;

    // 4️⃣ Validar que todo exista
    if (!contactId || !locationId || !timeframe) {
      return res.status(400).json({ error: 'Faltan datos (contactId, locationId o TimeFrame)' });
    }

    // 5️⃣ TimeFrame llega tipo "60 to 300" → sacamos mínimo y máximo
    const match = timeframe.match(/(\d+(?:\.\d+)?)\s*to\s*(\d+(?:\.\d+)?)/i);
    if (!match) {
      return res.status(400).json({ error: 'TimeFrame mal formado, usa "60 to 300"' });
    }
    const min  = parseFloat(match[1]);
    const max  = parseFloat(match[2]);

    // 6️⃣ Elegimos delay aleatorio entre min y max
    const delaySeconds = Math.floor(Math.random() * (max - min + 1)) + min;
    const delayMs      = delaySeconds * 1000;
    console.log(`⏱️ Delay aleatorio: ${delaySeconds}s`);

    // 7️⃣ Buscar el custom field "timerdone" en GHL
    const fieldRes = await fetch(`https://gh-connector.vercel.app/proxy/locations/${locationId}/customFields`, {
      headers: {
        Authorization: process.env.GHL_API_KEY || '',
        LocationId:    locationId
      }
    });
    const fieldsArr = await fieldRes.json();
    const allFields = Array.isArray(fieldsArr) ? fieldsArr : fieldsArr.customFields;
    const timerField = allFields.find((f: any) => f.name?.toLowerCase() === 'timerdone');

    if (!timerField) {
      return res.status(500).json({ error: 'Custom field "timerdone" no encontrado en GHL' });
    }

    // 8️⃣ Metemos el trabajo a la cola con su delay exacto
    await contactQueue.add(
      'ghl-contact',
      {
        contactId,
        locationId,
        customFieldId: timerField.id
      },
      {
        delay: delayMs,
        jobId: `${contactId}-${Date.now()}`
      }
    );

    console.log(`✅ Contacto ${contactId} encolado con ${delaySeconds}s`);
    return res.status(200).json({ success: true });

  } catch (err: any) {
    console.error('🔥 ERROR ENCOLANDO:', err.stack || err.message || err);
    return res.status(500).json({ error: 'Error interno del servidor' });
  }
}
