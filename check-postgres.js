import { Pool } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

// Conexión a PostgreSQL usando DATABASE_URL en .env
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function checkPostgres() {
  let client;
  try {
    console.log('Comprobando conexión a PostgreSQL...');
    client = await pool.connect();
    
    // Ver qué tablas existen
    const tableRes = await client.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    );
    console.log('Tablas en la base de datos:');
    tableRes.rows.forEach(row => console.log(`- ${row.table_name}`));
    
    // Ver estructura de sequential_queue
    console.log('\nEstructura de sequential_queue:');
    const structureRes = await client.query(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'sequential_queue' ORDER BY ordinal_position"
    );
    structureRes.rows.forEach(row => console.log(`- ${row.column_name}: ${row.data_type}`));
    
    // Contar filas en sequential_queue
    const countRes = await client.query('SELECT COUNT(*) FROM sequential_queue');
    console.log(`\nTotal filas en sequential_queue: ${countRes.rows[0].count}`);
    
    // Ver últimos 10 registros ordenados por run_at
    console.log('\nÚltimos 10 registros en sequential_queue (ordenados por run_at):');
    const latestRes = await client.query(`
      SELECT 
        id, 
        contact_id, 
        location_id, 
        delay_seconds,
        run_at,
        COALESCE(workflow_id, 'NULL') as workflow_id,
        NOW() as current_time,
        EXTRACT(EPOCH FROM (run_at - NOW()))/60 as delay_minutes
      FROM sequential_queue 
      ORDER BY run_at DESC 
      LIMIT 10
    `);
    
    latestRes.rows.forEach(row => {
      console.log(`ID: ${row.id}`);
      console.log(`  Contact ID: ${row.contact_id}`);
      console.log(`  Location ID: ${row.location_id}`);
      console.log(`  Workflow ID: ${row.workflow_id}`);
      console.log(`  Delay Seconds: ${row.delay_seconds}`);
      console.log(`  Run At: ${row.run_at}`);
      console.log(`  Delay Minutes: ${Math.round(row.delay_minutes)} mins`);
      console.log('---');
    });
    
    // Ver estructura de location_custom_fields
    console.log('\nEstructura de location_custom_fields:');
    const fieldStructureRes = await client.query(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'location_custom_fields' ORDER BY ordinal_position"
    );
    fieldStructureRes.rows.forEach(row => console.log(`- ${row.column_name}: ${row.data_type}`));
    
    // Contar filas en location_custom_fields
    const fieldCountRes = await client.query('SELECT COUNT(*) FROM location_custom_fields');
    console.log(`\nTotal filas en location_custom_fields: ${fieldCountRes.rows[0].count}`);
    
    // Ver registros en location_custom_fields
    console.log('\nRegistros en location_custom_fields:');
    const fieldRes = await client.query('SELECT * FROM location_custom_fields LIMIT 10');
    fieldRes.rows.forEach(row => {
      console.log(`Location ID: ${row.location_id}`);
      console.log(`  Custom Field ID: ${row.timerdone_custom_field_id}`);
      console.log(`  Created At: ${row.created_at}`);
      console.log('---');
    });
    
  } catch (error) {
    console.error('Error al consultar PostgreSQL:', error);
  } finally {
    if (client) client.release();
    await pool.end();
  }
}

checkPostgres().catch(console.error); 