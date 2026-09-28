export type AccountStatus = 'ACTIVE' | 'FROZEN' | 'CLOSED';
export type TransactionType = 'TRANSFER' | 'DEPOSIT' | 'WITHDRAWAL' | 'FEE' | 'ADJUSTMENT';
export type TransactionStatus = 'COMMITTED' | 'REVERSED';
export type EntryDirection = 'DEBIT' | 'CREDIT';
export type OutboxStatus = 'PENDING' | 'PUBLISHED' | 'FAILED';

export interface Account {
  id: string;
  account_number: string;
  currency: string;
  status: AccountStatus;
  cached_balance: string; // BIGINT serialized as string from pg
  created_at: Date;
  updated_at: Date;
}

export interface Transaction {
  id: string;
  correlation_id: string;
  type: TransactionType;
  description?: string | null;
  status: TransactionStatus;
  created_at: Date;
}

export interface LedgerEntry {
  id: string;
  transaction_id: string;
  account_id: string;
  direction: EntryDirection;
  amount_cents: string; // BIGINT serialized as string
  currency: string;
  balance_after: string; // BIGINT serialized as string
  created_at: Date;
}

export interface IdempotencyRecord {
  idempotency_key: string;
  transaction_id?: string | null;
  request_hash: string;
  status_code: number;
  response_body: Record<string, unknown>;
  created_at: Date;
  expires_at: Date;
}

export interface OutboxMessage {
  id: string;
  aggregate_type: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown>;
  status: OutboxStatus;
  retry_count: number;
  created_at: Date;
  published_at?: Date | null;
}

/**
 * Imposes a strict total order (canonical lexicographical sorting) on account IDs.
 * This guarantees the lock acquisition graph is a Directed Acyclic Graph (DAG),
 * mathematically eliminating Coffman's circular wait condition and database deadlocks.
 */
export function getCanonicalLockOrder(accountA: string, accountB: string): [string, string] {
  if (accountA === accountB) {
    throw new Error('Cannot execute transfer between the same account');
  }
  return accountA.localeCompare(accountB) < 0 ? [accountA, accountB] : [accountB, accountA];
}
