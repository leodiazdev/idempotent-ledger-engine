import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { pool } from './pool.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export async function runMigrations() {
  const migrationsDir = path.resolve(__dirname, '../../../migrations');
  console.log(`[Migrations] Scanning directory: ${migrationsDir}`);

  if (!fs.existsSync(migrationsDir)) {
    throw new Error(`Migrations directory does not exist: ${migrationsDir}`);
  }

  const files = fs.readdirSync(migrationsDir).filter((f) => f.endsWith('.sql')).sort();
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    for (const file of files) {
      console.log(`[Migrations] Applying ${file}...`);
      const filePath = path.join(migrationsDir, file);
      const sql = fs.readFileSync(filePath, 'utf8');
      await client.query(sql);
      console.log(`[Migrations] Applied ${file} successfully.`);
    }
    await client.query('COMMIT');
    console.log('[Migrations] All database migrations applied successfully.');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[Migrations] Migration failed, transaction rolled back:', err);
    throw err;
  } finally {
    client.release();
  }
}

if (process.argv[1] && process.argv[1].endsWith('migrate.ts')) {
  runMigrations()
    .then(() => {
      console.log('[Migrations] Completed.');
      process.exit(0);
    })
    .catch((err) => {
      console.error('[Migrations] Fatal error:', err);
      process.exit(1);
    });
}
