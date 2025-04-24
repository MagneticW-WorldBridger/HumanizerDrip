// worker.ts — duende que procesa los contactos UNO POR UNO 🧑‍🔧

// 1️⃣ Traemos BullMQ y la conexión a Redis
import { Worker } from 'bullmq';
import dotenv from 'dotenv';
dotenv.config();

// 2️⃣ Configuramos la conexión a Upstash Redis
const connection = {
  host: process.env.UPSTASH_REDIS_REST_URL!,
  password: process.env.UPSTASH_REDIS_REST_TOKEN!,
  tls: {}            // cifrado
};

// 3️⃣ Función que hará el update en GHL
async function updateContact({ contactId, locationId, customFieldId }: any) {
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
        customFields: [
          { id: customFieldId, field_value: 'YES' }
        ]
      })
    }
  );

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`❌ Falló GHL (${res.status}): ${body}`);
  }

  console.log(`✅ Contacto ${contactId} actualizado`);
}

// 4️⃣ Creamos el Worker de BullMQ
new Worker(
  'contactos',                // nombre de la cola
  async (job) => {
    await updateContact(job.data);   // procesa el trabajo
  },
  {
    connection,
    concurrency: 1            // SIEMPRE secuencial
  }
);

// 5️⃣ Mensaje para saber que arrancó
console.log('👂 Worker escuchando la cola "contactos"...');
