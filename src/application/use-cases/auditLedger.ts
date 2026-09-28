import { pool } from '../../infrastructure/db/pool.js';

export interface AuditReportDTO {
  is_valid: boolean;
  total_transactions: number;
  total_ledger_entries: number;
  total_debits_cents: number;
  total_credits_cents: number;
  net_delta_cents: number;
  unbalanced_transactions: Array<{
    transaction_id: string;
    total_debits: number;
    total_credits: number;
    difference: number;
  }>;
  audited_at: string;
}

export class AuditLedgerUseCase {
  async execute(): Promise<AuditReportDTO> {
    // 1. Check for any transaction where Sum(Debits) != Sum(Credits)
    const unbalancedRes = await pool.query<{
      transaction_id: string;
      total_debits: string;
      total_credits: string;
    }>(
      `SELECT 
         transaction_id,
         SUM(CASE WHEN direction = 'DEBIT' THEN amount_cents ELSE 0 END) AS total_debits,
         SUM(CASE WHEN direction = 'CREDIT' THEN amount_cents ELSE 0 END) AS total_credits
       FROM ledger_entries
       GROUP BY transaction_id
       HAVING SUM(CASE WHEN direction = 'DEBIT' THEN amount_cents ELSE 0 END) != 
              SUM(CASE WHEN direction = 'CREDIT' THEN amount_cents ELSE 0 END)`
    );

    // 2. Global totals
    const totalsRes = await pool.query<{
      tx_count: string;
      entry_count: string;
      total_debits: string;
      total_credits: string;
    }>(
      `SELECT 
         COUNT(DISTINCT transaction_id) AS tx_count,
         COUNT(*) AS entry_count,
         COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount_cents ELSE 0 END), 0) AS total_debits,
         COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_cents ELSE 0 END), 0) AS total_credits
       FROM ledger_entries`
    );

    const totals = totalsRes.rows[0];
    const totalDebits = Number(totals.total_debits);
    const totalCredits = Number(totals.total_credits);
    const netDelta = totalDebits - totalCredits;

    const unbalanced = unbalancedRes.rows.map((r) => ({
      transaction_id: r.transaction_id,
      total_debits: Number(r.total_debits),
      total_credits: Number(r.total_credits),
      difference: Number(r.total_debits) - Number(r.total_credits),
    }));

    return {
      is_valid: unbalanced.length === 0,
      total_transactions: Number(totals.tx_count),
      total_ledger_entries: Number(totals.entry_count),
      total_debits_cents: totalDebits,
      total_credits_cents: totalCredits,
      net_delta_cents: netDelta,
      unbalanced_transactions: unbalanced,
      audited_at: new Date().toISOString(),
    };
  }
}
