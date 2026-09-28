import Fastify, { FastifyInstance } from 'fastify';
import cors from '@fastify/cors';
import sensible from '@fastify/sensible';
import { transferRoutes } from './api/routes/transferRoutes.js';
import { accountRoutes } from './api/routes/accountRoutes.js';
import { auditRoutes } from './api/routes/auditRoutes.js';
import { pool } from './infrastructure/db/pool.js';
import { getIsRedisConnected } from './infrastructure/redis/client.js';

export async function buildServer(): Promise<FastifyInstance> {
  const fastify = Fastify({
    logger: {
      level: process.env.LOG_LEVEL || 'info',
      transport:
        process.env.NODE_ENV !== 'production'
          ? {
              target: 'pino-pretty',
              options: {
                colorize: true,
                translateTime: 'HH:MM:ss Z',
                ignore: 'pid,hostname',
              },
            }
          : undefined,
    },
  });

  await fastify.register(cors);
  await fastify.register(sensible);

  // Health check endpoint
  fastify.get('/health', async (_req, reply) => {
    let dbStatus = 'DOWN';
    try {
      await pool.query('SELECT 1');
      dbStatus = 'UP';
    } catch {
      dbStatus = 'DOWN';
    }

    const redisStatus = getIsRedisConnected() ? 'UP' : 'DEGRADED';
    const isHealthy = dbStatus === 'UP';

    return reply.status(isHealthy ? 200 : 503).send({
      status: isHealthy ? 'UP' : 'DOWN',
      services: {
        database: dbStatus,
        redis: redisStatus,
      },
      timestamp: new Date().toISOString(),
    });
  });

  // Register API domain routes
  await fastify.register(transferRoutes);
  await fastify.register(accountRoutes);
  await fastify.register(auditRoutes);

  return fastify;
}
