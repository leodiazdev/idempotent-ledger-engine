import { FastifyRequest, FastifyReply } from 'fastify';
import { DistributedIdempotencyMutex } from '../../infrastructure/redis/mutex.js';

declare module 'fastify' {
  interface FastifyRequest {
    idempotencyKey?: string;
  }
}

export async function idempotencyPreHandler(
  request: FastifyRequest,
  reply: FastifyReply
): Promise<void> {
  const idempotencyKey = request.headers['idempotency-key'];

  if (!idempotencyKey || typeof idempotencyKey !== 'string') {
    return; // No idempotency key provided, proceed normally
  }

  const cleanKey = idempotencyKey.trim();
  if (cleanKey.length === 0) {
    return;
  }

  request.idempotencyKey = cleanKey;

  // 1. Fast-path check: Is this idempotency key already resolved and cached?
  const cached = await DistributedIdempotencyMutex.getResolvedResponse(cleanKey);
  if (cached) {
    reply
      .header('Idempotency-Key', cleanKey)
      .header('X-Cache', 'HIT')
      .status(cached.statusCode)
      .send(cached.body);
    return reply;
  }

  // 2. In-flight check: Is another request with this key currently executing?
  const lockAcquired = await DistributedIdempotencyMutex.acquireInFlightLock(cleanKey);
  if (!lockAcquired) {
    reply.status(409).send({
      error: 'CONCURRENT_REQUEST_IN_FLIGHT',
      message: `A transfer with Idempotency-Key '${cleanKey}' is currently being processed. Please await completion.`,
    });
    return reply;
  }
}
