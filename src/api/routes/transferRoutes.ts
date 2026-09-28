import { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { TransferFundsUseCase } from '../../application/use-cases/transferFunds.js';
import { idempotencyPreHandler } from '../middleware/idempotency.js';
import { DistributedIdempotencyMutex } from '../../infrastructure/redis/mutex.js';
import { DomainError } from '../../domain/errors.js';

const transferSchema = z.object({
  source_account_id: z.string().min(1, 'source_account_id is required'),
  destination_account_id: z.string().min(1, 'destination_account_id is required'),
  amount_cents: z
    .number()
    .int('amount_cents must be an integer')
    .positive('amount_cents must be strictly positive'),
  currency: z.string().length(3, 'currency must be a 3-letter ISO code'),
  description: z.string().max(255).optional(),
});

export async function transferRoutes(fastify: FastifyInstance) {
  const transferUseCase = new TransferFundsUseCase();

  fastify.post(
    '/api/v1/transfers',
    {
      preHandler: idempotencyPreHandler,
    },
    async (request, reply) => {
      const parseResult = transferSchema.safeParse(request.body);
      if (!parseResult.success) {
        if (request.idempotencyKey) {
          await DistributedIdempotencyMutex.release(request.idempotencyKey);
        }
        return reply.status(400).send({
          error: 'VALIDATION_ERROR',
          issues: parseResult.error.format(),
        });
      }

      try {
        const result = await transferUseCase.execute({
          ...parseResult.data,
          idempotency_key: request.idempotencyKey,
        });

        if (request.idempotencyKey) {
          reply.header('Idempotency-Key', request.idempotencyKey);
        }

        return reply
          .header('Location', `/api/v1/transfers/${result.data.transaction_id}`)
          .status(result.statusCode)
          .send(result.data);
      } catch (err) {
        // If domain error or unexpected error occurred, release lock so the client may retry
        if (request.idempotencyKey) {
          await DistributedIdempotencyMutex.release(request.idempotencyKey);
        }

        if (err instanceof DomainError) {
          return reply.status(err.statusCode).send({
            error: err.errorCode,
            message: err.message,
          });
        }

        request.log.error(err, 'Unexpected error executing transfer');
        return reply.status(500).send({
          error: 'INTERNAL_SERVER_ERROR',
          message: 'An unexpected error occurred during ledger settlement',
        });
      }
    }
  );
}
