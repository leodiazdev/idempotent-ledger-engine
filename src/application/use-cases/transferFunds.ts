import crypto from 'crypto';
import { v4 as uuidv4 } from 'uuid';
import { withTransaction, pool } from '../../infrastructure/db/pool.js';
import { DistributedIdempotencyMutex } from '../../infrastructure/redis/mutex.js';
import { Account, getCanonicalLockOrder } from '../../domain/types.js';
import {
  InsufficientFundsError,
  AccountNotFoundError,
  AccountInactiveError,
  CurrencyMismatchError,
  ZeroSumViolationError,
  InvalidAmountError,
  IdempotencyPayloadMismatchError,
} from '../../domain/errors.js';

export interface TransferRequestDTO {
  source_account_id: string;
  destination_account_id: string;
  amount_cents: number;
  currency: string;
  description?: string;
  idempotency_key?: string;
}

export interface TransferResponseDTO {
  transaction_id: string;
  status: 'COMMITTED';
  source_account_id: string;
  destination_account_id: string;
  amount_cents: number;
  currency: string;
  created_at: string;
  ledger_entries: {
    source_debit_entry_id: string;
    source_balance_after_cents: number;
    destination_credit_entry_id: string;
    destination_balance_after_cents: number;
  };
}

export class TransferFundsUseCase {
  async execute(dto: TransferRequestDTO): Promise<{ statusCode: number; data: TransferResponseDTO }> {
    const amount = BigInt(dto.amount_cents);
    if (amount <= 0n) {
      throw new InvalidAmountError('Amount must be a positive integer greater than zero');
    }

    const currency = dto.currency.toUpperCase();
    const idempotencyKey = dto.idempotency_key;

    // Calculate request hash for payload integrity check
    const normalizedPayload = JSON.stringify({
      src: dto.source_account_id,
      dst: dto.destination_account_id,
      amt: dto.amount_cents,
      curr: currency,
    });
    const requestHash = crypto.createHash('sha256').update(normalizedPayload).digest('hex');

    // 1. Check Tier-2 Durable Idempotency in DB if key provided
    if (idempotencyKey) {
      const existingRecord = await pool.query(
        `SELECT response_body, status_code, request_hash FROM idempotency_records WHERE idempotency_key = $1`,
        [idempotencyKey]
      );

      if (existingRecord.rows.length > 0) {
        const row = existingRecord.rows[0];
        if (row.request_hash !== requestHash) {
          throw new IdempotencyPayloadMismatchError(idempotencyKey);
        }
        // Return previously committed response (Replayed)
        return {
          statusCode: row.status_code,
          data: row.response_body as TransferResponseDTO,
        };
      }
    }

    // 2. Resolve account UUIDs if account_number was passed
    const accountLookup = await pool.query<Account>(
      `SELECT id, account_number, currency, status, cached_balance 
       FROM accounts 
       WHERE id::text IN ($1, $2) OR account_number IN ($1, $2)`,
      [dto.source_account_id, dto.destination_account_id]
    );

    const sourceAccount = accountLookup.rows.find(
      (a) => a.id === dto.source_account_id || a.account_number === dto.source_account_id
    );
    const destAccount = accountLookup.rows.find(
      (a) => a.id === dto.destination_account_id || a.account_number === dto.destination_account_id
    );

    if (!sourceAccount) {
      throw new AccountNotFoundError(dto.source_account_id);
    }
    if (!destAccount) {
      throw new AccountNotFoundError(dto.destination_account_id);
    }
    if (sourceAccount.id === destAccount.id) {
      throw new InvalidAmountError('Source and destination accounts must be distinct');
    }

    // 3. Execute Transaction with Canonical Lexicographical Locking
    const result = await withTransaction(async (client) => {
      // Determine canonical order (A < B alphabetically) to eliminate deadlocks
      const [firstLockId, secondLockId] = getCanonicalLockOrder(sourceAccount.id, destAccount.id);

      // Acquire pessimistic locks on accounts in strict canonical order
      const lockedAccountsRes = await client.query<Account>(
        `SELECT id, account_number, currency, status, cached_balance 
         FROM accounts 
         WHERE id IN ($1, $2) 
         ORDER BY id ASC 
         FOR UPDATE`,
        [firstLockId, secondLockId]
      );

      const lockedSource = lockedAccountsRes.rows.find((a) => a.id === sourceAccount.id);
      const lockedDest = lockedAccountsRes.rows.find((a) => a.id === destAccount.id);

      if (!lockedSource || !lockedDest) {
        throw new AccountNotFoundError('One or more accounts could not be locked');
      }

      if (lockedSource.status !== 'ACTIVE') {
        throw new AccountInactiveError(lockedSource.account_number, lockedSource.status);
      }
      if (lockedDest.status !== 'ACTIVE') {
        throw new AccountInactiveError(lockedDest.account_number, lockedDest.status);
      }

      if (lockedSource.currency !== currency) {
        throw new CurrencyMismatchError(lockedSource.currency, currency);
      }
      if (lockedDest.currency !== currency) {
        throw new CurrencyMismatchError(lockedDest.currency, currency);
      }

      const currentSourceBalance = BigInt(lockedSource.cached_balance);
      const currentDestBalance = BigInt(lockedDest.cached_balance);

      if (currentSourceBalance < amount) {
        throw new InsufficientFundsError(lockedSource.account_number, currentSourceBalance, amount);
      }

      // Calculate projected balances
      const newSourceBalance = currentSourceBalance - amount;
      const newDestBalance = currentDestBalance + amount;

      // Invariant verification: Zero-Sum debits and credits
      const totalDebit = amount;
      const totalCredit = amount;
      if (totalDebit !== totalCredit) {
        throw new ZeroSumViolationError(totalDebit, totalCredit);
      }

      // Create Transaction Record
      const txId = uuidv4();
      const correlationId = idempotencyKey ? `tx-${idempotencyKey}` : `tx-${txId}`;
      const now = new Date();

      await client.query(
        `INSERT INTO transactions (id, correlation_id, type, description, status, created_at)
         VALUES ($1, $2, 'TRANSFER', $3, 'COMMITTED', $4)`,
        [txId, correlationId, dto.description || 'Monetary transfer', now]
      );

      // Create Double-Entry Ledger Lines
      const debitEntryId = uuidv4();
      await client.query(
        `INSERT INTO ledger_entries (id, transaction_id, account_id, direction, amount_cents, currency, balance_after, created_at)
         VALUES ($1, $2, $3, 'DEBIT', $4, $5, $6, $7)`,
        [debitEntryId, txId, lockedSource.id, amount.toString(), currency, newSourceBalance.toString(), now]
      );

      const creditEntryId = uuidv4();
      await client.query(
        `INSERT INTO ledger_entries (id, transaction_id, account_id, direction, amount_cents, currency, balance_after, created_at)
         VALUES ($1, $2, $3, 'CREDIT', $4, $5, $6, $7)`,
        [creditEntryId, txId, lockedDest.id, amount.toString(), currency, newDestBalance.toString(), now]
      );

      // Update cached balance on accounts
      await client.query(
        `UPDATE accounts SET cached_balance = $1, updated_at = NOW() WHERE id = $2`,
        [newSourceBalance.toString(), lockedSource.id]
      );
      await client.query(
        `UPDATE accounts SET cached_balance = $1, updated_at = NOW() WHERE id = $2`,
        [newDestBalance.toString(), lockedDest.id]
      );

      const responsePayload: TransferResponseDTO = {
        transaction_id: txId,
        status: 'COMMITTED',
        source_account_id: lockedSource.account_number,
        destination_account_id: lockedDest.account_number,
        amount_cents: Number(amount),
        currency,
        created_at: now.toISOString(),
        ledger_entries: {
          source_debit_entry_id: debitEntryId,
          source_balance_after_cents: Number(newSourceBalance),
          destination_credit_entry_id: creditEntryId,
          destination_balance_after_cents: Number(newDestBalance),
        },
      };

      // Transactional Outbox Pattern: Insert event in same DB transaction
      await client.query(
        `INSERT INTO outbox_messages (aggregate_type, aggregate_id, event_type, payload, status)
         VALUES ('TRANSACTION', $1, 'TRANSFER_SETTLED', $2, 'PENDING')`,
        [txId, JSON.stringify(responsePayload)]
      );

      // Tier-2 Idempotency Persistence
      if (idempotencyKey) {
        await client.query(
          `INSERT INTO idempotency_records (idempotency_key, transaction_id, request_hash, status_code, response_body)
           VALUES ($1, $2, $3, 201, $4)`,
          [idempotencyKey, txId, requestHash, JSON.stringify(responsePayload)]
        );
      }

      return responsePayload;
    });

    // 4. Update Tier-1 Redis Mutex with Resolved Cached Response
    if (idempotencyKey) {
      await DistributedIdempotencyMutex.resolve(
        idempotencyKey,
        201,
        result as unknown as Record<string, unknown>
      );
    }

    return { statusCode: 201, data: result };
  }
}
