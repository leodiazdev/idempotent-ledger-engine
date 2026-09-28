import { pool } from '../../infrastructure/db/pool.js';
import { Account, LedgerEntry } from '../../domain/types.js';
import { AccountNotFoundError } from '../../domain/errors.js';

export interface AccountBalanceDTO {
  account_id: string;
  account_number: string;
  currency: string;
  status: string;
  cached_balance_cents: number;
  calculated_balance_cents: number;
  history: Array<{
    entry_id: string;
    direction: string;
    amount_cents: number;
    balance_after_cents: number;
    created_at: string;
  }>;
}

export class GetBalanceUseCase {
  async execute(accountIdentifier: string): Promise<AccountBalanceDTO> {
    const accRes = await pool.query<Account>(
      `SELECT * FROM accounts WHERE id::text = $1 OR account_number = $1 LIMIT 1`,
      [accountIdentifier]
    );

    if (accRes.rows.length === 0) {
      throw new AccountNotFoundError(accountIdentifier);
    }

    const account = accRes.rows[0];

    // Query ledger entries to reconstruct balance from history
    const entriesRes = await pool.query<LedgerEntry>(
      `SELECT * FROM ledger_entries WHERE account_id = $1 ORDER BY created_at DESC LIMIT 20`,
      [account.id]
    );

    // Sum credits minus debits from ledger to verify zero-drift
    const sumRes = await pool.query<{ total: string }>(
      `SELECT 
         COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_cents ELSE -amount_cents END), 0) AS total
       FROM ledger_entries 
       WHERE account_id = $1`,
      [account.id]
    );

    const calculatedBalance = Number(sumRes.rows[0]?.total || 0);

    return {
      account_id: account.id,
      account_number: account.account_number,
      currency: account.currency,
      status: account.status,
      cached_balance_cents: Number(account.cached_balance),
      calculated_balance_cents: calculatedBalance,
      history: entriesRes.rows.map((e) => ({
        entry_id: e.id,
        direction: e.direction,
        amount_cents: Number(e.amount_cents),
        balance_after_cents: Number(e.balance_after),
        created_at: e.created_at.toISOString(),
      })),
    };
  }
}
