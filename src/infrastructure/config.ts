import dotenv from 'dotenv';
dotenv.config();

export const config = {
  port: parseInt(process.env.PORT || '8080', 10),
  host: process.env.HOST || '0.0.0.0',
  nodeEnv: process.env.NODE_ENV || 'development',
  databaseUrl: process.env.DATABASE_URL || 'postgres://postgres:postgrespassword@localhost:5432/ledger_db',
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6379',
  outboxPollIntervalMs: parseInt(process.env.OUTBOX_POLL_INTERVAL_MS || '500', 10),
  outboxBatchSize: parseInt(process.env.OUTBOX_BATCH_SIZE || '100', 10),
  idempotencyTtlSeconds: 86400, // 24 hours
  inFlightTtlSeconds: 60,       // 60 seconds
};
