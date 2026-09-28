import { buildServer } from './server.js';
import { config } from './infrastructure/config.js';
import { initRedis, redis } from './infrastructure/redis/client.js';
import { pool } from './infrastructure/db/pool.js';
import { runMigrations } from './infrastructure/db/migrate.js';
import { OutboxWorker } from './workers/outboxWorker.js';

async function bootstrap() {
  console.log('[Bootstrap] Initializing Idempotent Ledger Engine...');

  // Initialize Redis
  await initRedis();

  // Run database migrations on startup if PostgreSQL is reachable
  try {
    await runMigrations();
  } catch (err) {
    console.warn('[Bootstrap] Database migrations not applied on boot (will retry on container start):', (err as Error).message);
  }

  // Start background outbox polling worker
  const outboxWorker = new OutboxWorker();
  await outboxWorker.start();

  // Build and start HTTP server
  const server = await buildServer();

  try {
    await server.listen({ port: config.port, host: config.host });
    console.log(`[Bootstrap] HTTP Server running on http://${config.host}:${config.port}`);
  } catch (err) {
    server.log.error(err);
    process.exit(1);
  }

  // Graceful shutdown handling
  const shutdown = async (signal: string) => {
    console.log(`[Shutdown] Received ${signal}. Gracefully stopping engine...`);
    outboxWorker.stop();
    await server.close();
    await pool.end();
    if (redis.status === 'ready') {
      await redis.quit();
    }
    console.log('[Shutdown] Engine cleanly terminated.');
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

bootstrap().catch((err) => {
  console.error('[Bootstrap] Fatal startup error:', err);
  process.exit(1);
});
