import { describe, it, expect } from 'vitest';
import { getCanonicalLockOrder } from '../src/domain/types.js';

interface SimulatedAccount {
  id: string;
  account_number: string;
  balance_cents: bigint;
  lockQueue: Array<() => void>;
  isLocked: boolean;
}

interface SimulatedLedgerEntry {
  transaction_id: string;
  account_id: string;
  direction: 'DEBIT' | 'CREDIT';
  amount_cents: bigint;
  balance_after: bigint;
}

/**
 * Concurrency Test Harness:
 * Simulates high-contention bidirectional transfers across 50 concurrent worker promises.
 * Proves that canonical lock ordering eliminates cyclic wait conditions (deadlocks)
 * while preserving total system value and double-entry invariants.
 */
class ConcurrentLedgerSimulator {
  accounts = new Map<string, SimulatedAccount>();
  ledgerEntries: SimulatedLedgerEntry[] = [];
  deadlockCount = 0;

  createAccount(id: string, initialBalance: bigint): SimulatedAccount {
    const acc: SimulatedAccount = {
      id,
      account_number: `ACC-${id}`,
      balance_cents: initialBalance,
      lockQueue: [],
      isLocked: false,
    };
    this.accounts.set(id, acc);
    return acc;
  }

  // Pessimistic locking simulator with lock acquisition timeout detection
  private async acquireAccountLock(account: SimulatedAccount, timeoutMs = 2000): Promise<() => void> {
    if (!account.isLocked) {
      account.isLocked = true;
      return () => {
        const next = account.lockQueue.shift();
        if (next) {
          next();
        } else {
          account.isLocked = false;
        }
      };
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.deadlockCount++;
        reject(new Error(`LOCK_TIMEOUT_DEADLOCK_DETECTED on account ${account.id}`));
      }, timeoutMs);

      account.lockQueue.push(() => {
        clearTimeout(timer);
        resolve(() => {
          const next = account.lockQueue.shift();
          if (next) {
            next();
          } else {
            account.isLocked = false;
          }
        });
      });
    });
  }

  async executeTransfer(
    sourceId: string,
    destId: string,
    amountCents: bigint,
    txId: string
  ): Promise<void> {
    // Canonical Ordering to prevent deadlocks: min(A, B), max(A, B)
    const [firstId, secondId] = getCanonicalLockOrder(sourceId, destId);

    const firstAcc = this.accounts.get(firstId)!;
    const secondAcc = this.accounts.get(secondId)!;

    // Acquire locks in canonical order
    const releaseFirst = await this.acquireAccountLock(firstAcc);
    const releaseSecond = await this.acquireAccountLock(secondAcc);

    try {
      const source = this.accounts.get(sourceId)!;
      const dest = this.accounts.get(destId)!;

      if (source.balance_cents < amountCents) {
        throw new Error('INSUFFICIENT_FUNDS');
      }

      // Debit source, Credit destination
      source.balance_cents -= amountCents;
      dest.balance_cents += amountCents;

      // Double-entry record
      this.ledgerEntries.push({
        transaction_id: txId,
        account_id: source.id,
        direction: 'DEBIT',
        amount_cents: amountCents,
        balance_after: source.balance_cents,
      });

      this.ledgerEntries.push({
        transaction_id: txId,
        account_id: dest.id,
        direction: 'CREDIT',
        amount_cents: amountCents,
        balance_after: dest.balance_cents,
      });
    } finally {
      releaseSecond();
      releaseFirst();
    }
  }
}

describe('Concurrency Verification: 50 Concurrent Bidirectional Transfers', () => {
  it('should execute 50 high-contention cross-transfers with 0 deadlocks and exact value conservation', async () => {
    const simulator = new ConcurrentLedgerSimulator();

    const initialA = 10_000_000n; // $100,000.00
    const initialB = 10_000_000n; // $100,000.00
    const totalExpectedSystemBalance = initialA + initialB; // 20,000,000 cents ($200,000.00)

    const accA = simulator.createAccount('uuid-acc-001-A', initialA);
    const accB = simulator.createAccount('uuid-acc-002-B', initialB);

    const CONCURRENT_TRANSFERS = 50;
    const transferAmount = 10000n; // $100.00 per transfer

    const transferTasks: Promise<void>[] = [];

    for (let i = 0; i < CONCURRENT_TRANSFERS; i++) {
      const txId = `tx-stress-${i}`;
      // Alternate direction simultaneously: Even transfers A -> B, Odd transfers B -> A
      if (i % 2 === 0) {
        transferTasks.push(simulator.executeTransfer(accA.id, accB.id, transferAmount, txId));
      } else {
        transferTasks.push(simulator.executeTransfer(accB.id, accA.id, transferAmount, txId));
      }
    }

    // Await all 50 concurrent transactions
    await Promise.all(transferTasks);

    // 1. Assert Deadlock Immunity
    expect(simulator.deadlockCount).toBe(0);

    // 2. Assert Closed-Loop Value Conservation Invariant
    const finalBalanceA = simulator.accounts.get(accA.id)!.balance_cents;
    const finalBalanceB = simulator.accounts.get(accB.id)!.balance_cents;
    const finalTotalBalance = finalBalanceA + finalBalanceB;

    expect(finalTotalBalance).toBe(totalExpectedSystemBalance);

    // Since 25 transfers went A -> B and 25 transfers went B -> A of identical amounts,
    // final balances should exactly equal initial balances:
    expect(finalBalanceA).toBe(initialA);
    expect(finalBalanceB).toBe(initialB);

    // 3. Assert Double-Entry Invariant (Sum Debits == Sum Credits for every single transaction)
    const txMap = new Map<string, { debits: bigint; credits: bigint }>();
    for (const entry of simulator.ledgerEntries) {
      if (!txMap.has(entry.transaction_id)) {
        txMap.set(entry.transaction_id, { debits: 0n, credits: 0n });
      }
      const record = txMap.get(entry.transaction_id)!;
      if (entry.direction === 'DEBIT') {
        record.debits += entry.amount_cents;
      } else {
        record.credits += entry.amount_cents;
      }
    }

    expect(txMap.size).toBe(CONCURRENT_TRANSFERS);
    for (const [, sums] of txMap) {
      expect(sums.debits).toBe(transferAmount);
      expect(sums.credits).toBe(transferAmount);
      expect(sums.debits - sums.credits).toBe(0n);
    }
  });
});
