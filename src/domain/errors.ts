export abstract class DomainError extends Error {
  abstract readonly statusCode: number;
  abstract readonly errorCode: string;

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class InsufficientFundsError extends DomainError {
  readonly statusCode = 422;
  readonly errorCode = 'INSUFFICIENT_FUNDS';

  constructor(accountId: string, availableCents: bigint, requestedCents: bigint) {
    super(
      `Account '${accountId}' has insufficient available balance (available: ${availableCents.toString()} cents, requested: ${requestedCents.toString()} cents)`
    );
  }
}

export class AccountNotFoundError extends DomainError {
  readonly statusCode = 404;
  readonly errorCode = 'ACCOUNT_NOT_FOUND';

  constructor(accountId: string) {
    super(`Account with identifier '${accountId}' was not found`);
  }
}

export class AccountInactiveError extends DomainError {
  readonly statusCode = 422;
  readonly errorCode = 'ACCOUNT_INACTIVE';

  constructor(accountId: string, status: string) {
    super(`Account '${accountId}' is in status '${status}' and cannot participate in transfers`);
  }
}

export class CurrencyMismatchError extends DomainError {
  readonly statusCode = 422;
  readonly errorCode = 'CURRENCY_MISMATCH';

  constructor(expected: string, actual: string) {
    super(`Currency mismatch: account uses '${expected}', but transfer requested '${actual}'`);
  }
}

export class ZeroSumViolationError extends DomainError {
  readonly statusCode = 500;
  readonly errorCode = 'ZERO_SUM_VIOLATION';

  constructor(debits: bigint, credits: bigint) {
    super(
      `Ledger invariant violated: Total debits (${debits.toString()}) does not equal total credits (${credits.toString()})`
    );
  }
}

export class InvalidAmountError extends DomainError {
  readonly statusCode = 400;
  readonly errorCode = 'INVALID_AMOUNT';

  constructor(message = 'Transfer amount must be a positive integer in minor units (cents)') {
    super(message);
  }
}

export class ConcurrentRequestError extends DomainError {
  readonly statusCode = 409;
  readonly errorCode = 'CONCURRENT_REQUEST_IN_FLIGHT';

  constructor(idempotencyKey: string) {
    super(
      `A transfer with Idempotency-Key '${idempotencyKey}' is currently in-flight. Please wait and retry.`
    );
  }
}

export class IdempotencyPayloadMismatchError extends DomainError {
  readonly statusCode = 422;
  readonly errorCode = 'IDEMPOTENCY_PAYLOAD_MISMATCH';

  constructor(idempotencyKey: string) {
    super(
      `Idempotency-Key '${idempotencyKey}' was previously used with a different request payload.`
    );
  }
}
