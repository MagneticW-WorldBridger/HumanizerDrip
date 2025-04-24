import { Worker } from 'bullmq';
import IORedis from 'ioredis';
import dotenv from 'dotenv';
dotenv.config();

// 2️⃣ Conexión a Redis con TLS (la rediss://… que pusiste en .env)
const redis = new IORedis(process.env.REDIS_URL!, {
  maxRetriesPerRequest: null
});

// 3️⃣ Función para llamar a GHL
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

// 4️⃣ Creamos el Worker que escucha la cola “contactos”
new Worker(
  'contactos',
  async job => {
    await updateContact(job.data);
  },
  {
    connection: redis,
    concurrency: 1
  }
);

// 5️⃣ Mensaje para saber que arrancó
console.log('👂 Worker escuchando la cola "contactos"...');
