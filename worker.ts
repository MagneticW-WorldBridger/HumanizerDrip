import { Worker, Job } from 'bullmq';
import IORedis from 'ioredis';
import { Pool } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

// 1️⃣ Conexión a Redis
const redis = new IORedis(process.env.REDIS_URL!, {
  maxRetriesPerRequest: null,
  tls: {}
});

// 2️⃣ Conexión a PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
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

// 4️⃣ Función para borrar el contacto de la tabla
async function removeFromQueue(contactId: string, locationId: string) {
  const client = await pool.connect();
  try {
    await client.query(
      `DELETE FROM sequential_queue
         WHERE contact_id = $1
           AND location_id = $2`,
      [contactId, locationId]
    );
    console.log(`🗑️  Contacto ${contactId} borrado de sequential_queue`);
  } finally {
    client.release();
  }
}

// 5️⃣ Worker que escucha la cola “contactos” y borra tras éxitos
new Worker(
  'contactos',
  async (job: Job) => {
    const { contactId, locationId, customFieldId } = job.data;
    await updateContact(job.data);
    // solo después de que PUT a GHL haya ido bien:
    await removeFromQueue(contactId, locationId);
  },
  {
    connection: redis,
    concurrency: 1
  }
);

console.log('👂 Worker escuchando la cola "contactos"...');