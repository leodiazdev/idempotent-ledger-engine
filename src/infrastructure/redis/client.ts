import { Redis } from 'ioredis';
import { config } from '../config.js';

export const redis = new Redis(config.redisUrl, {
  maxRetriesPerRequest: 3,
  retryStrategy(times: number) {
    const delay = Math.min(times * 100, 2000);
    return delay;
  },
  lazyConnect: true,
});

let isRedisConnected = false;

redis.on('connect', () => {
  isRedisConnected = true;
  console.log('[Redis] Connected to Redis cluster/node');
});

redis.on('error', (err: Error) => {
  isRedisConnected = false;
  // Non-fatal warning so the app can start or log without crashing outright if Redis is starting up
  console.warn('[Redis] Connection warning:', err.message);
});

export async function initRedis(): Promise<boolean> {
  try {
    await redis.connect();
    return true;
  } catch (err) {
    console.warn('[Redis] Initial connect failed (running in degraded or local mode):', (err as Error).message);
    return false;
  }
}

export function getIsRedisConnected(): boolean {
  return isRedisConnected;
}
