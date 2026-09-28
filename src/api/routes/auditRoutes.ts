import { FastifyInstance } from 'fastify';
import { AuditLedgerUseCase } from '../../application/use-cases/auditLedger.js';

export async function auditRoutes(fastify: FastifyInstance) {
  const auditUseCase = new AuditLedgerUseCase();

  fastify.get('/api/v1/audit/ledger-integrity', async (_request, reply) => {
    try {
      const report = await auditUseCase.execute();
      return reply.status(report.is_valid ? 200 : 500).send(report);
    } catch (err) {
      _request.log.error(err, 'Failed to audit ledger');
      return reply.status(500).send({
        error: 'AUDIT_FAILED',
        message: 'Could not complete ledger audit query',
      });
    }
  });
}
