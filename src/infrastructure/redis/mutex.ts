import { redis, getIsRedisConnected } from './client.js';
import { config } from '../config.js';

interface CachedResponse {
  statusCode: number;
  body: Record<string, unknown>;
}

// In-memory fallback if Redis is unavailable during certain local test environments
const memoryMutex = new Map<string, { status: 'PROCESSING' | 'RESOLVED'; data?: CachedResponse; expiresAt: number }>();

export class DistributedIdempotencyMutex {
  private static prefix = 'idemp:';

  /**
   * Attempts to acquire an atomic in-flight mutex lock in Redis.
   * Returns true if lock was successfully acquired, false if a request with this key is already in flight.
   */
  static async acquireInFlightLock(key: string): Promise<boolean> {
    const fullKey = `${this.prefix}${key}`;
    if (getIsRedisConnected()) {
      try {
        const result = await redis.set(fullKey, 'PROCESSING', 'EX', config.inFlightTtlSeconds, 'NX');
        return result === 'OK';
      } catch (err) {
        console.warn('[Idempotency Mutex] Redis error acquiring lock, falling back to memory:', (err as Error).message);
      }
    }

    // Memory fallback
    const now = Date.now();
    const existing = memoryMutex.get(fullKey);
    if (existing && existing.expiresAt > now) {
      return false; // Lock already held
    }

    memoryMutex.set(fullKey, {
      status: 'PROCESSING',
      expiresAt: now + config.inFlightTtlSeconds * 1000,
    });
    return true;
  }

  /**
   * Stores the final committed response in Redis cache for instant replaying.
   */
  static async resolve(key: string, statusCode: number, body: Record<string, unknown>): Promise<void> {
    const fullKey = `${this.prefix}${key}`;
    const payload: CachedResponse = { statusCode, body };
    const serialized = JSON.stringify(payload);

    if (getIsRedisConnected()) {
      try {
        await redis.set(fullKey, `RESOLVED:${serialized}`, 'EX', config.idempotencyTtlSeconds);
        return;
      } catch (err) {
        console.warn('[Idempotency Mutex] Redis error resolving lock:', (err as Error).message);
      }
    }

    memoryMutex.set(fullKey, {
      status: 'RESOLVED',
      data: payload,
      expiresAt: Date.now() + config.idempotencyTtlSeconds * 1000,
    });
  }

  /**
   * Checks if an idempotency key was previously resolved and cached.
   */
  static async getResolvedResponse(key: string): Promise<CachedResponse | null> {
    const fullKey = `${this.prefix}${key}`;
    if (getIsRedisConnected()) {
      try {
        const raw = await redis.get(fullKey);
        if (raw && raw.startsWith('RESOLVED:')) {
          const jsonStr = raw.substring('RESOLVED:'.length);
          return JSON.parse(jsonStr) as CachedResponse;
        }
      } catch (err) {
        console.warn('[Idempotency Mutex] Redis error reading resolved response:', (err as Error).message);
      }
    }

    const item = memoryMutex.get(fullKey);
    if (item && item.status === 'RESOLVED' && item.expiresAt > Date.now() && item.data) {
      return item.data;
    }

    return null;
  }

  /**
   * Releases an in-flight lock if a transaction failed before completion.
   */
  static async release(key: string): Promise<void> {
    const fullKey = `${this.prefix}${key}`;
    if (getIsRedisConnected()) {
      try {
        await redis.del(fullKey);
      } catch (err) {
        console.warn('[Idempotency Mutex] Redis error deleting key:', (err as Error).message);
      }
    }
    memoryMutex.delete(fullKey);
  }
}
