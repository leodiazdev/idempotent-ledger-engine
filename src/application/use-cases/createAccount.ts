import { v4 as uuidv4 } from 'uuid';
import { withTransaction, pool } from '../../infrastructure/db/pool.js';
import { Account } from '../../domain/types.js';

export interface CreateAccountDTO {
  account_number: string;
  currency: string;
  initial_balance_cents?: number;
}

export class CreateAccountUseCase {
  async execute(dto: CreateAccountDTO): Promise<Account> {
    const currency = dto.currency.toUpperCase();
    const initialBalance = BigInt(dto.initial_balance_cents || 0);

    return await withTransaction(async (client) => {
      // Create Account
      const accountRes = await client.query<Account>(
        `INSERT INTO accounts (account_number, currency, status, cached_balance)
         VALUES ($1, $2, 'ACTIVE', $3)
         RETURNING *`,
        [dto.account_number, currency, initialBalance.toString()]
      );

      const account = accountRes.rows[0];

      // If there is initial funding, create an initial DEPOSIT transaction and ledger entry
      if (initialBalance > 0n) {
        const txId = uuidv4();
        await client.query(
          `INSERT INTO transactions (id, correlation_id, type, description, status)
           VALUES ($1, $2, 'DEPOSIT', 'Initial account funding', 'COMMITTED')`,
          [txId, `init-${account.id}`]
        );

        await client.query(
          `INSERT INTO ledger_entries (transaction_id, account_id, direction, amount_cents, currency, balance_after)
           VALUES ($1, $2, 'CREDIT', $3, $4, $5)`,
          [txId, account.id, initialBalance.toString(), currency, initialBalance.toString()]
        );
      }

      return account;
    });
  }

  async getAccountById(idOrNumber: string): Promise<Account | null> {
    const res = await pool.query<Account>(
      `SELECT * FROM accounts WHERE id::text = $1 OR account_number = $1 LIMIT 1`,
      [idOrNumber]
    );
    return res.rows[0] || null;
  }
}
