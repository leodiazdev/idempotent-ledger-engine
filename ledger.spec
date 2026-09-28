### Phase 6: Production-Grade Architectural Documentation (`main`)

On branch `main`, generate an exhaustive, enterprise-grade `README.md`. It must not be a basic template; it must read like an internal architecture RFC from a Tier-1 fintech company.

The `README.md` must include the following sections:

#### 1. System Overview & Problem Statement
- Explain the business and technical challenge: guaranteeing strictly consistent, fault-tolerant monetary transfers over unreliable networks without double-spending or distributed race conditions.

#### 2. Architectural Decision Records (ADRs) & Trade-offs
Detail the rationale and trade-offs behind each critical design decision:
- **Double-Entry Bookkeeping vs. Mutable Balances:** Why keeping immutable credit/debit lines prevents ledger corruption, enables zero-sum auditing ($\sum \text{Debits} - \sum \text{Credits} = 0$), and avoids state drift.
- **Canonical Pessimistic Locking vs. Optimistic Locking:** Why sorting account IDs alphabetically before acquiring `SELECT ... FOR UPDATE` locks completely eliminates deadlocks in high-contention bidirectional transfers.
- **Layered Distributed Idempotency (Redis + PostgreSQL):** Why a dual-layer approach (Redis memory mutex for fast flight checks + PostgreSQL persistence for finalized responses) protects against network retries and duplicate HTTP requests.
- **Transactional Outbox with `SKIP LOCKED` vs. Dual-Writes / 2PC:** Why writing events directly to an outbox table in the same DB transaction guarantees at-least-once message delivery without two-phase commit overhead.
- **Minor-Unit Integer Representation (Cents) vs. Floats:** The mathematical and precision risks of IEEE 754 floating-point types in financial software.

#### 3. Visual Architecture & Sequence Diagrams (Mermaid)
You must render valid Mermaid diagrams for:
1. **System Topology & Hexagonal Layers:**
   - Client -> API Layer (Filters, Interceptors) -> Application Use Cases -> Domain Core -> Persistence (PostgreSQL, Redis) & Messaging (Outbox Worker -> Broker).
2. **Idempotent Transfer Request Lifecycle (Sequence Diagram):**
   - Sequence showing: Client sending `POST` with `Idempotency-Key` -> Redis Lock check (`PROCESSING` check) -> DB Transaction (`BEGIN`) -> Canonical sorting & `FOR UPDATE` locking of Account A & B -> Double-entry balance calculation -> Ledger write -> Outbox record insert -> DB `COMMIT` -> Redis state update to `RESOLVED` -> HTTP 201 Response.
3. **Transactional Outbox Polling & Dispatch (Sequence Diagram):**
   - Sequence showing: Cron trigger -> Worker querying `outbox_messages` using `FOR UPDATE SKIP LOCKED` -> Publish batch to Kafka/RabbitMQ -> Await Broker ACK -> Mark records `PUBLISHED` -> Commit transaction.
4. **Data Model / Entity Relationship Diagram (ERD):**
   - Showing relations between `accounts`, `transactions`, `ledger_entries`, `idempotency_records`, and `outbox_messages`.

#### 4. Concurrency Verification & Benchmark Guide
- Document how the Testcontainers test suite works:
  - Spawning isolated PostgreSQL and Redis instances.
  - Simulating 50 concurrent transfers against identical and competing accounts.
  - Asserting the ledger invariant, zero balance corruption, and zero deadlocks.

#### 5. Local Setup & Quickstart
- Single-command execution: `docker-compose up -d`.
- Step-by-step instructions to run migrations, unit tests, and integration/concurrency test suites.
- Example `curl` requests with headers (`Idempotency-Key`) and payloads demonstrating the transfer flow and duplicate request rejection.