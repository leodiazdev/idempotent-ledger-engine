import { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { CreateAccountUseCase } from '../../application/use-cases/createAccount.js';
import { GetBalanceUseCase } from '../../application/use-cases/getBalance.js';
import { DomainError } from '../../domain/errors.js';

const createAccountSchema = z.object({
  account_number: z.string().min(3).max(64),
  currency: z.string().length(3),
  initial_balance_cents: z.number().int().nonnegative().optional(),
});

export async function accountRoutes(fastify: FastifyInstance) {
  const createAccountUseCase = new CreateAccountUseCase();
  const getBalanceUseCase = new GetBalanceUseCase();

  fastify.post('/api/v1/accounts', async (request, reply) => {
    const parseResult = createAccountSchema.safeParse(request.body);
    if (!parseResult.success) {
      return reply.status(400).send({
        error: 'VALIDATION_ERROR',
        issues: parseResult.error.format(),
      });
    }

    try {
      const account = await createAccountUseCase.execute(parseResult.data);
      return reply.status(201).send(account);
    } catch (err: unknown) {
      if (err instanceof DomainError) {
        return reply.status(err.statusCode).send({
          error: err.errorCode,
          message: err.message,
        });
      }
      if ((err as { code?: string })?.code === '23505') {
        return reply.status(409).send({
          error: 'ACCOUNT_ALREADY_EXISTS',
          message: `Account number '${parseResult.data.account_number}' already exists`,
        });
      }
      request.log.error(err, 'Failed to create account');
      return reply.status(500).send({
        error: 'INTERNAL_SERVER_ERROR',
        message: 'Could not create account',
      });
    }
  });

  fastify.get('/api/v1/accounts/:id/balance', async (request, reply) => {
    const { id } = request.params as { id: string };

    try {
      const balance = await getBalanceUseCase.execute(id);
      return reply.status(200).send(balance);
    } catch (err: unknown) {
      if (err instanceof DomainError) {
        return reply.status(err.statusCode).send({
          error: err.errorCode,
          message: err.message,
        });
      }
      request.log.error(err, 'Failed to get balance');
      return reply.status(500).send({
        error: 'INTERNAL_SERVER_ERROR',
        message: 'Could not fetch account balance',
      });
    }
  });
}
