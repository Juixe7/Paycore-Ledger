# PayCore — Staff Engineer Interview Defense Guide (`INTERVIEW_DEFENSE.md`)

This guide prepares you to defend every design decision, SQL query, concurrency primitive, and failure mode in **PayCore** during system design and backend engineering interviews.

---

## Q1: Why did you use explicit `JdbcClient` SQL instead of Spring Data JPA / Hibernate?
**Answer**:
On a financial money path, ORMs introduce three subtle hazards:
1. **Implicit First-Level Cache (Persistence Context) Staleness**: Reading an entity before acquiring a pessimistic lock can return cached state rather than the latest committed row version.
2. **Hidden Lock Ordering & Flush Timing**: In [`LedgerService.post`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/java/com/paycore/payments/ledger/LedgerService.java), we must execute `SELECT ... FROM accounts WHERE id = ? FOR UPDATE` in **strictly sorted `account_id` order** before inserting into `ledger_entries`. Hibernate's action queue reorders `INSERT`/`UPDATE` statements unless manually flushed.
3. **PostgreSQL-Specific Concurrency Primitives**: We rely heavily on `INSERT ... ON CONFLICT DO NOTHING`, `FOR UPDATE SKIP LOCKED` (in [`OutboxPollingRelay`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/java/com/paycore/payments/outbox/OutboxPollingRelay.java) and [`WebhookDeliveryService`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/webhook-service/src/main/java/com/paycore/webhook/service/WebhookDeliveryService.java)), and partitioned table inserts (`ledger_entries`, `outbox_events`). Spring Boot 3.3's `JdbcClient` gives us zero-magic, parameterized SQL with clean Java 21 record mapping.

---

## Q2: Why is `authorizePayment` split into Two Phases instead of a single `@Transactional` method?
**Answer**:
If `@Transactional` wraps the entire `authorizePayment` method, the thread checks out a JDBC connection from HikariCP (`maximum-pool-size = 32`), inserts the payment row, and **holds that database connection idle** while waiting 80ms–2,000ms for the external PSP HTTP call.
- With 32 concurrent slow PSP calls, the entire database pool is exhausted, starving fast operations like `capture`, `getPayment`, and health checks.
- Instead, [`IdempotencyService.executeTwoPhase`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/java/com/paycore/payments/service/IdempotencyService.java) and [`PaymentService.authorizePayment`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/java/com/paycore/payments/service/PaymentService.java) execute:
  - **Tx 1 (`REQUIRES_NEW`)**: Insert `idempotency_keys` (`status = 'IN_PROGRESS'`) + `payments` (`status = 'CREATED'`). Commit immediately (< 3ms), returning the connection to HikariCP.
  - **External PSP HTTP Call**: Runs with **zero database connections held** on a JDK 21 Virtual Thread.
  - **Tx 2 (`REQUIRES_NEW`)**: Lock the `payments` row, update status to `AUTHORIZED` (or `FAILED`), write `outbox_events`, and store the final HTTP response snapshot in `idempotency_keys` (`status = 'COMPLETED'`).
- **Empirical Proof ([`StretchAbBenchmarkTest`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/test/java/com/paycore/payments/StretchAbBenchmarkTest.java))**: Under 48 concurrent authorizations with 80ms PSP latency, Virtual Threads + Two-Phase Idempotency completed in **`295ms`** vs **`790ms`** (`2.68x` faster) with zero connection pool timeouts.

---

## Q3: What happens if a client sends 500 concurrent requests with the exact same `Idempotency-Key`?
**Answer**:
In [`IdempotencyService`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/java/com/paycore/payments/service/IdempotencyService.java):
1. We execute an atomic `INSERT INTO idempotency_keys (merchant_id, idempotency_key, request_hash, status, ...) VALUES (..., 'IN_PROGRESS', ...) ON CONFLICT DO NOTHING`.
2. Exactly **1** thread wins the insert (`rowsInserted == 1`) and proceeds to execute the operation.
3. The other **499** threads see `rowsInserted == 0` and inspect the existing row:
   - If `request_hash` differs from the stored SHA-256 payload hash -> `422 Unprocessable Entity` (`IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_PAYLOAD`).
   - If `status == 'IN_PROGRESS'` -> `409 Conflict` (`IDEMPOTENCY_KEY_IN_FLIGHT`), telling the client the first request is currently executing.
   - If `status == 'COMPLETED'` -> replays the exact cached status code and JSON body without re-executing business logic.
- **Empirical Proof ([`ConcurrencyAndCrashTortureTest`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/test/java/com/paycore/payments/ConcurrencyAndCrashTortureTest.java))**: 500 concurrent requests resulted in **1** PSP call and **1** payment created.

---

## Q4: How do you prevent the "Over-Refund Race Condition" when 100 concurrent threads request partial refunds?
**Answer**:
Suppose a payment has `captured_paise = 100,000` (₹1,000) and 100 concurrent requests each ask to refund `20,000` paise (₹200).
- **Naive Unlocked Approach**: All 100 threads read `refunded_amount = 0` at the same instant, see `0 + 20,000 <= 100,000`, and all 100 post ledger refunds—resulting in ₹20,000 refunded on a ₹1,000 payment (**20x over-refund**).
- **PayCore's Approach ([`PaymentService.refundPayment`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/java/com/paycore/payments/service/PaymentService.java))**:
  1. Acquire an exclusive row lock: `SELECT ... FROM payments WHERE id = ? FOR UPDATE`.
  2. Concurrent threads block at the PostgreSQL row lock until the holding transaction commits.
  3. When each waiting thread wakes up, PostgreSQL `READ COMMITTED` re-evaluates the `SELECT ... FOR UPDATE` against the newly committed version of the row (`refunded_amount = 20000`, then `40000`, ..., `100000`).
  4. Threads 1–5 succeed; threads 6–100 see `100,000 + 20,000 > 100,000` and fail fast with `422 OVER_REFUND_REJECTED`.
- **Empirical Proof**: Demonstrated side-by-side in [`ConcurrencyAndCrashTortureTest`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/test/java/com/paycore/payments/ConcurrencyAndCrashTortureTest.java) (`100/100` in naive mode vs `5/100` in locked mode).

---

## Q5: How does the Double-Entry Ledger guarantee `SUM(DEBIT) == SUM(CREDIT)` even if application code has a bug?
**Answer**:
We use defense-in-depth across three layers:
1. **Application Check ([`LedgerService.post`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/java/com/paycore/payments/ledger/LedgerService.java))**: Verifies at least 2 legs, all amounts `> 0`, same currency, and `totalDebits == totalCredits` using exact `Math.addExact` integer arithmetic.
2. **ArchUnit Test ([`ArchitectureBoundaryTest`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/test/java/com/paycore/payments/ArchitectureBoundaryTest.java))**: Fails the build if any class outside `..ledger..` accesses internal ledger classes or raw SQL on `ledger_entries`.
3. **PostgreSQL Deferred Constraint Trigger ([`V1__init_payments_core_schema.sql`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/resources/db/migration/V1__init_payments_core_schema.sql))**:
   - `CREATE CONSTRAINT TRIGGER trg_verify_transaction_balanced AFTER INSERT ON ledger_entries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION verify_ledger_transaction_balanced();`
   - Because it is `DEFERRABLE INITIALLY DEFERRED`, PostgreSQL waits until the end of the SQL transaction (`COMMIT`) after all legs are inserted, then verifies `SUM(DEBIT) == SUM(CREDIT)` for that `txn_id`. If unbalanced, PostgreSQL aborts the commit.

---

## Q6: How do you avoid AB-BA Deadlocks when posting multi-leg ledger transactions?
**Answer**:
Suppose Transaction 1 transfers from Account `B` to Account `A`, while Transaction 2 transfers from Account `A` to Account `B`. If they lock rows in the order the legs were passed, Tx 1 locks `B` and waits for `A`, while Tx 2 locks `A` and waits for `B` -> **Deadlock**.
- In [`LedgerService.post`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/java/com/paycore/payments/ledger/LedgerService.java), we extract all distinct `accountId`s from the posting legs and **sort them lexicographically (`naturalOrder()`)** before executing `SELECT ... FROM accounts WHERE id = ? FOR UPDATE`.
- Every transaction in the system acquires locks in the exact same global order (`A` then `B`), making cycle formation mathematically impossible.

---

## Q7: Why did you shard `PSP_RECEIVABLE:INR` into 16 shards (`00..0f`), and why didn't you shard `MERCHANT_PAYABLE` by default?
**Answer**:
- Every single payment capture in the entire platform debits `PSP_RECEIVABLE:INR`. Without sharding, `PSP_RECEIVABLE:INR` is a global serialization bottleneck: 1,000 merchants capturing payments simultaneously all queue on a single PostgreSQL row lock.
- By sharding `PSP_RECEIVABLE:INR` into 16 sub-accounts (`PSP_RECEIVABLE:INR:00` through `0f`) routed deterministically by `MurmurHash3(paymentId) & 15`, we cut lock contention by 16x while keeping a given payment's capture and subsequent refunds on the **same shard**.
- **Measured Result ([`StretchAbBenchmarkTest`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/test/java/com/paycore/payments/StretchAbBenchmarkTest.java))**: Under 64 concurrent captures, 16-shard mode achieved **`p50 = 261ms` vs `1,165ms` (`4.46x` faster)** and **`p99 = 392ms` vs `1,768ms` (`4.51x` faster)**.
- Why not shard `MERCHANT_PAYABLE` by default? Because payouts (`POST /v1/payouts`) must enforce a strict **non-negative balance check (`balance >= payoutAmount`)** on the merchant's payable account. If a merchant's balance were split across 16 shards, a payout would have to lock all 16 shards to check the aggregate balance.

---

## Q8: How do you guarantee that a committed Ledger Transaction never fails to publish a Kafka event (Dual-Write Problem)?
**Answer**:
We never publish to Kafka inside the request thread. Instead:
1. Inside the same PostgreSQL ACID transaction that updates `payments` and inserts `ledger_entries`, [`OutboxWriter`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/java/com/paycore/payments/outbox/OutboxWriter.java) inserts a serialized Protobuf `PaymentEvent` into the `outbox_events` table with a gapless per-payment `event_seq` (`payments.last_event_seq + 1`).
2. Either **Debezium CDC** ([`debezium-outbox-connector.json`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/deploy/k8s/debezium-outbox-connector.json)) tails PostgreSQL WAL, or [`OutboxPollingRelay`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/java/com/paycore/payments/outbox/OutboxPollingRelay.java) polls unpublished rows using `SELECT ... WHERE published_at IS NULL ORDER BY created_at ASC LIMIT ? FOR UPDATE SKIP LOCKED`.
3. Because outbox relay is **at-least-once**, downstream consumers (`read-model-service` and `webhook-service`) deduplicate by `event_id` (`UUIDv7`).

---

## Q9: How does `read-model-service` handle out-of-order Kafka events (e.g., `event_seq = 2` arriving before `event_seq = 1`)?
**Answer**:
In [`ReadModelProjectorService`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/read-model-service/src/main/java/com/paycore/readmodel/service/ReadModelProjectorService.java):
1. Each `payment_view` row tracks `last_applied_seq` (initially `0` before `event_seq = 1`).
2. When an event arrives with `event_seq`:
   - If `event_seq <= last_applied_seq`: it is a duplicate/already-applied event -> ignore idempotently.
   - If `event_seq == last_applied_seq + 1`: apply immediately to `payment_view`, increment `last_applied_seq`, and then **drain any consecutive buffered events (`last_applied_seq + 1, +2, ...`)** from `projection_event_buffer` in the same transaction.
   - If `event_seq > last_applied_seq + 1`: a gap exists! Insert the event into `projection_event_buffer (payment_id, event_seq, payload)` and wait for the missing predecessor.
- Verified in [`ReadModelIntegrationTest`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/read-model-service/src/test/java/com/paycore/readmodel/ReadModelIntegrationTest.java).

---

## Q10: How does `webhook-service` protect against SSRF and verify authenticity for merchants?
**Answer**:
1. **SSRF Protection ([`WebhookSsrfValidator`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/webhook-service/src/main/java/com/paycore/webhook/service/WebhookSsrfValidator.java))**: Before registering or delivering to a webhook URL, we resolve its DNS host and reject loopback (`127.0.0.0/8`, `::1`), link-local / cloud metadata (`169.254.169.254`), site-local private RFC1918 ranges (`10/8`, `172.16/12`, `192.168/16`), and non-HTTP(S) schemes.
2. **HMAC-SHA256 Signing ([`WebhookSigner`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/webhook-service/src/main/java/com/paycore/webhook/service/WebhookSigner.java))**: Computes `HMAC-SHA256(webhookSecret, timestamp + "." + rawBody)` and sends `X-PayCore-Signature: t=<epochSeconds>,v1=<hexSignature>`. Including the timestamp in the signed payload prevents replay attacks outside a 5-minute tolerance window.
3. **Exponential Backoff & DLQ**: Failed deliveries schedule retries with exponential backoff; after `MAX_ATTEMPTS = 5`, the delivery transitions to `DLQ` and can be replayed via `POST /v1/webhooks/dlq/{deliveryId}/replay`.

---

## Q11: Why use `UUIDv7` instead of `UUIDv4` or `BIGSERIAL`?
**Answer**:
- `UUIDv4` is completely random (122 random bits), causing severe **B-Tree index page splits and buffer-pool thrashing** in PostgreSQL as table size grows, because every new row lands in a random leaf page.
- `BIGSERIAL` is sequential but leaks business volume/enumeration and requires central sequence coordination.
- [`UuidV7`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/platform-common/src/main/java/com/paycore/common/ids/UuidV7.java) embeds a **48-bit Unix epoch millisecond timestamp** in the most significant bits followed by a monotonic sub-millisecond counter and cryptographic randomness. New inserts always append to the right-most B-Tree page, and cursor pagination (`WHERE (created_at, payment_id) < (?, ?)`) is naturally time-ordered.

---

## Q12: How does the T+1 Settlement Reconciliation Engine detect silent financial discrepancies?
**Answer**:
Even with an ACID internal ledger, external drift can happen (e.g., PSP settles a different fee, a network timeout caused an unknown capture at the PSP, or a settlement file omits a transaction).
[`ReconciliationService`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/reconciliation/src/main/java/com/paycore/reconciliation/service/ReconciliationService.java) ingests the PSP's T+1 settlement batch against our internal ledger snapshot for that window and classifies every record into:
1. `MATCHED`: Present in both, amounts and terminal statuses match to the exact paise.
2. `MISSING_IN_LEDGER`: PSP settled a charge that our ledger never recorded as captured.
3. `MISSING_IN_PSP_FILE`: Our ledger recorded a capture, but the PSP settlement file omitted it.
4. `AMOUNT_MISMATCH`: Both recorded the transaction, but settled paise != ledger paise.
5. `STATUS_MISMATCH`: e.g., PSP marks `CAPTURED` while internal state is `REFUNDED` or `AUTHORIZED`.

Key implementation details:
- **Date-scoped query**: `loadInternalPayments(LocalDate)` filters `WHERE updated_at >= :startOfDay AND updated_at < :endOfDay` — no full table scan across the partitioned `ledger_entries` table.
- **Net amount reconciliation**: For `PARTIALLY_REFUNDED` or `REFUNDED` payments, `expectedSettlementAmount = capturedAmount − refundedAmount`. The PSP settles only the net amount after refunds, so comparing gross `capturedAmount` would always show a false `AMOUNT_MISMATCH`.
- **Status compatibility**: A payment in `PARTIALLY_REFUNDED` or `REFUNDED` state is compatible with PSP status `SETTLED` — the PSP simply settled whatever amount remained after refunds.
- **`MISSING_IN_PSP_FILE` detection**: Any of our `CAPTURED`, `PARTIALLY_REFUNDED`, or `REFUNDED` payments not present in the PSP file are flagged — this catches silent PSP omissions that could mean we captured money the PSP never actually collected.

---

## Q13: What happens to a `PARTIALLY_REFUNDED` payment during reconciliation? Walk me through the state machine.

**Answer**:
This is a subtle edge case that reveals whether a state machine is fully thought through. Suppose a payment for ₹1,000 has had two ₹100 refunds applied:
- `captured_paise = 100,000`, `refunded_paise = 20,000`, status = `PARTIALLY_REFUNDED`

**Internal state machine transitions ([`PaymentStatus`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/java/com/paycore/payments/payment/PaymentStatus.java)):**
```
CAPTURED           →  PARTIALLY_REFUNDED  (first partial refund; refunded < captured)
PARTIALLY_REFUNDED →  PARTIALLY_REFUNDED  (another partial refund; still refunded < captured)
PARTIALLY_REFUNDED →  REFUNDED            (final refund that brings refunded == captured)
CAPTURED           →  REFUNDED            (single full refund in one shot)
```
The transition is determined atomically by a DB-level `CASE` expression in [`PaymentService.refundPayment`](file:///c:/Users/Asus/Desktop/ProjectsFInal/Paycore/payments-core/src/main/java/com/paycore/payments/payment/PaymentService.java):
```sql
UPDATE payments
SET status = CASE
    WHEN refunded_paise + :refundAmount = captured_paise THEN 'REFUNDED'
    ELSE 'PARTIALLY_REFUNDED'
END
WHERE id = :id AND status IN ('CAPTURED', 'PARTIALLY_REFUNDED')
```
This is inside the same transaction holding the `FOR UPDATE` row lock, so the status and `refunded_paise` update atomically — no window for a concurrent thread to see an intermediate state.

**During reconciliation**, the PSP settlement file will contain one line for this payment:
```
psp_ref_XYZ, 80000, SETTLED, 2026-10-11
```
(PSP settled `100,000 − 20,000 = 80,000 paise` net, because refunds are applied before settlement.)

`ReconciliationService.expectedSettlementAmount()` computes `capturedAmount − refundedAmount = 80,000 paise`, which matches the PSP file exactly → `MATCHED`. If we had naively compared `capturedAmount = 100,000` against the PSP's `80,000` we'd incorrectly flag every refunded payment as `AMOUNT_MISMATCH`.

**Verified by `ReconciliationIntegrationTest`** which inserts a `PARTIALLY_REFUNDED` payment with `capturedAmount=100000`, `refundedAmount=20000` and asserts the reconciliation result is `MATCHED` against a PSP file containing `80000`.
