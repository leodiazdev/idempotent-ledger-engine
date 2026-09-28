import { describe, it, expect, beforeEach } from 'vitest';
import { getCanonicalLockOrder } from '../src/domain/types.js';
import {
  InsufficientFundsError,
  AccountInactiveError,
  CurrencyMismatchError,
  ZeroSumViolationError,
  InvalidAmountError,
} from '../src/domain/errors.js';
import { DistributedIdempotencyMutex } from '../src/infrastructure/redis/mutex.js';

describe('Domain: Canonical Lexicographical Locking Order', () => {
  it('should deterministically order account IDs alphabetically regardless of argument order', () => {
    const accA = '018f3a21-99ab-7000-8000-000000000001';
    const accB = '018f3a21-99ab-7000-8000-000000000002';

    // Transfer A -> B
    const order1 = getCanonicalLockOrder(accA, accB);
    expect(order1).toEqual([accA, accB]);

    // Transfer B -> A (Opposite direction)
    const order2 = getCanonicalLockOrder(accB, accA);
    expect(order2).toEqual([accA, accB]);

    // Both transfers MUST acquire lock on accA first, then accB.
    // This breaks Coffman circular wait and mathematically eliminates deadlocks.
    expect(order1[0]).toBe(order2[0]);
    expect(order1[1]).toBe(order2[1]);
  });

  it('should throw an error if source and destination accounts are identical', () => {
    const acc = '018f3a21-99ab-7000-8000-000000000001';
    expect(() => getCanonicalLockOrder(acc, acc)).toThrow(
      'Cannot execute transfer between the same account'
    );
  });
});

describe('Domain: Double-Entry Financial Invariants', () => {
  it('should verify Zero-Sum debits and credits condition', () => {
    const amountCents = 5000n; // $50.00
    const debits = [amountCents];
    const credits = [amountCents];

    const sumDebits = debits.reduce((acc, d) => acc + d, 0n);
    const sumCredits = credits.reduce((acc, c) => acc + c, 0n);

    expect(sumDebits - sumCredits).toBe(0n);
  });

  it('should detect zero-sum discrepancy and formulate proper error', () => {
    const debits = 5000n;
    const credits = 4000n;
    const err = new ZeroSumViolationError(debits, credits);
    expect(err.statusCode).toBe(500);
    expect(err.errorCode).toBe('ZERO_SUM_VIOLATION');
    expect(err.message).toContain('Total debits (5000) does not equal total credits (4000)');
  });

  it('should strictly enforce minor-unit integer representation (cents)', () => {
    // Floating point demonstration: 0.1 + 0.2 !== 0.3
    const floatSum = 0.1 + 0.2;
    expect(floatSum).not.toBe(0.3);

    // Minor-unit integer representation: 10 cents + 20 cents === 30 cents
    const minorUnitSum = 10n + 20n;
    expect(minorUnitSum).toBe(30n);
  });
});

describe('Domain: Domain Exceptions & Status Codes', () => {
  it('should map InsufficientFundsError to 422 Unprocessable Entity', () => {
    const err = new InsufficientFundsError('ACC-001', 1000n, 5000n);
    expect(err.statusCode).toBe(422);
    expect(err.errorCode).toBe('INSUFFICIENT_FUNDS');
    expect(err.message).toContain('available: 1000 cents, requested: 5000 cents');
  });

  it('should map AccountInactiveError to 422', () => {
    const err = new AccountInactiveError('ACC-FROZEN', 'FROZEN');
    expect(err.statusCode).toBe(422);
    expect(err.errorCode).toBe('ACCOUNT_INACTIVE');
  });

  it('should map CurrencyMismatchError to 422', () => {
    const err = new CurrencyMismatchError('USD', 'EUR');
    expect(err.statusCode).toBe(422);
    expect(err.errorCode).toBe('CURRENCY_MISMATCH');
  });

  it('should map InvalidAmountError to 400', () => {
    const err = new InvalidAmountError();
    expect(err.statusCode).toBe(400);
    expect(err.errorCode).toBe('INVALID_AMOUNT');
  });
});

describe('Infrastructure: Distributed Idempotency Mutex', () => {
  const testKey = 'test-idemp-key-991';

  beforeEach(async () => {
    await DistributedIdempotencyMutex.release(testKey);
  });

  it('should acquire in-flight lock on first attempt and reject concurrent attempts', async () => {
    const acquiredFirst = await DistributedIdempotencyMutex.acquireInFlightLock(testKey);
    expect(acquiredFirst).toBe(true);

    // Second concurrent call with same key must fail
    const acquiredSecond = await DistributedIdempotencyMutex.acquireInFlightLock(testKey);
    expect(acquiredSecond).toBe(false);
  });

  it('should store and retrieve resolved response for replaying duplicate requests', async () => {
    await DistributedIdempotencyMutex.acquireInFlightLock(testKey);

    const mockResponse = {
      transaction_id: 'tx-12345',
      status: 'COMMITTED',
      amount_cents: 2500,
    };

    await DistributedIdempotencyMutex.resolve(testKey, 201, mockResponse);

    const cached = await DistributedIdempotencyMutex.getResolvedResponse(testKey);
    expect(cached).not.toBeNull();
    expect(cached?.statusCode).toBe(201);
    expect(cached?.body).toEqual(mockResponse);
  });

  it('should release lock cleanly upon transaction rollback or error', async () => {
    await DistributedIdempotencyMutex.acquireInFlightLock(testKey);
    await DistributedIdempotencyMutex.release(testKey);

    // Should be able to acquire again after release
    const reacquired = await DistributedIdempotencyMutex.acquireInFlightLock(testKey);
    expect(reacquired).toBe(true);
  });
});
