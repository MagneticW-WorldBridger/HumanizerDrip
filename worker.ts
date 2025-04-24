import { Worker } from 'bullmq';
import IORedis from 'ioredis';
import dotenv from 'dotenv';
dotenv.config();

// 2️⃣ Creamos conexión REST a Upstash Redis
const redis = new IORedis(process.env.UPSTASH_REDIS_REST_URL!, {
  password: process.env.UPSTASH_REDIS_REST_TOKEN!,
  tls: {}  // cifrado
});

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

// 4️⃣ Worker escuchando la cola
new Worker(
  'contactos',
  async (job) => {
    await updateContact(job.data);
  },
  {
    connection: redis,
    concurrency: 1
  }
);

console.log('👂 Worker escuchando la cola "contactos"...');
