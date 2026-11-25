# go-document-processor

A Go service that sends documents in batches to an external party (the
Principal), receives the results through asynchronous callbacks, and retries
safely. The focus is correctness under concurrency: a global limit of 3
requests, atomic claims, idempotent callbacks, and no document left stuck
when processing is cancelled.

Repository layout:

| Path | Contents |
|---|---|
| `docproc/` | Domain, the `Service` with the 6 required functions, the `Repository` interface |
| `docproc/memory/` | In-memory repository (mutex), used by the unit tests |
| `docproc/postgres/` | PostgreSQL repository (GORM + pgx) and integration tests |
| `docproc/repotest/` | Contract tests run against both repositories |
| `httpapi/` | Callback endpoint (validation, HMAC, status codes) and batch endpoints |
| `principal/` | HTTP `PrincipalClient` (with `Idempotency-Key`) |
| `internal/simulator/` | Fake Principal for running the server locally |
| `cmd/docproc-server/` | Wiring, periodic recovery, graceful shutdown |
| `db/schema.sql` | Schema plus added columns/constraints/indexes, each with its reason |
| `db/queries.sql` | The 8 query answers (16 statements), all verified automatically |
| `docs/answers.md` | Essays A–D (transactions, locking, recovery, EXPLAIN) |
| `docs/design.md` | Flow, state machine, and design decisions in more detail |
| `docs/testing.md` | Step-by-step testing guide: automated tests, integration tests, `curl` scenarios, mutation checks |

## Running

Requires **Go 1.25+** (uses `testing/synctest` and `sync.WaitGroup.Go`).
A fuller testing guide is in [docs/testing.md](docs/testing.md).

```bash
go build ./...
go test -race ./...
go test -race -count=10 ./...
```

The PostgreSQL integration tests are skipped automatically when
`TEST_DATABASE_URL` is not set. To run them:

```bash
make db-up
TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5432/docproc_test?sslmode=disable" go test -race ./...
```

> These tests **drop and recreate** the tables in that database. Do not
> point them at a database that holds data you care about.

The integration tests cover the repository contract tests, end-to-end
Service scenarios on PostgreSQL, and `TestQueriesSQL`, which runs `PREPARE`
for every query in `db/queries.sql` against `db/schema.sql` (so a typo in a
column name in the SQL file is caught too).

Benchmark:

```bash
go test -run '^$' -bench . -benchmem ./docproc/
```

### Running the server

With no configuration the server uses the in-memory repository and the
simulated Principal (some requests are rejected, some callbacks are FAILED
or sent twice), so the whole flow can be tried right away:

```bash
go run ./cmd/docproc-server
```

```bash
curl -s -X POST localhost:8080/v1/batches -H 'Content-Type: application/json' \
  -d '{"batch_no":"INV-001","documents":[{"document_no":"D1","payload":{"amount":1000}},{"document_no":"D2","payload":{"amount":2500}}]}'

curl -s -X POST localhost:8080/v1/batches/INV-001/process   # 202, runs in the background
curl -s localhost:8080/v1/batches/INV-001                   # progress
curl -s -X POST localhost:8080/v1/batches/INV-001/retry     # FAILED -> PENDING
curl -s localhost:8080/metrics
```

With PostgreSQL: `make db-up` (the schema is applied from `db/schema.sql`
automatically), then
`DATABASE_URL=postgres://postgres:postgres@localhost:5432/docproc?sslmode=disable go run ./cmd/docproc-server`.

| Env | Default | Notes |
|---|---|---|
| `HTTP_ADDR` | `:8080` | |
| `DATABASE_URL` | empty | empty = in-memory |
| `PRINCIPAL_URL` | empty | empty = simulator |
| `CALLBACK_SECRET` | empty | HMAC-SHA256 secret for the `X-Principal-Signature: sha256=<hex>` header. **Required when `PRINCIPAL_URL` is set**; the server refuses to start without it, so a real deployment never accepts unsigned callbacks |
| `SUBMIT_TIMEOUT` | `30s` | timeout per request to the Principal |
| `LEASE_TIMEOUT` | `2m` | must be > `SUBMIT_TIMEOUT` |
| `RECOVERY_INTERVAL` | `30s` | `RecoverStale` interval |

## Requirements mapped to code and tests

| Requirement | Implementation | Proven by |
|---|---|---|
| Max 3 concurrent requests, global per service | `Service.sem` (channel buffered to 3) taken in `ProcessDocument`, so it applies across `ProcessBatch` calls and direct calls | `TestProcessBatch_MaxConcurrentSubmit`, `TestConcurrencyLimitIsGlobalAcrossBatches` |
| Max 3 attempts | `attempt_count < max_attempts` checked on claim and reset; CHECK constraint in the DB | `TestRetry_StopsAtMaxAttempts`, `TestRetry_RecoversBeforeLimit`, contract `ClaimRules` |
| Never processed twice by different workers | Atomic claim: mutex (memory), `UPDATE ... WHERE status='PENDING'` + `FOR UPDATE SKIP LOCKED` (postgres) | `TestProcessDocument_SameIDManyGoroutines`, contract `ClaimIsAtomic` |
| Idempotent callbacks | Map/UNIQUE `callback_id`, `INSERT ... ON CONFLICT DO NOTHING` in the same transaction as the update | `TestHandleCallback_Duplicate` (sequential and parallel), contract `CallbackIdempotentUnderConcurrency` |
| A late FAILED never overwrites SUCCESS | A FAILED callback only applies when `status = 'PROCESSING'`; every update after Submit is fenced by attempt | `TestHandleCallback_LateFailedKeepsSuccess`, `TestHandleCallback_SuccessAndFailedRace`, `TestHandleCallback_Transitions`, `TestProcessDocument_CallbackSettlesDuringSubmit`, contract `FencingByAttempt` |
| No races | All state sits behind a mutex or the DB; no mutable globals | `TestMixedOperations_NoRace` + the whole suite with `-race -count=10` |
| Context cancellation | Every function takes a ctx; a document cancelled during Submit goes back to PENDING using a separate ctx (`context.WithoutCancel`) | `TestProcessBatch_ContextCancelled`, `TestProcessBatch_AlreadyCancelledContext` |
| SUCCESS is not processed again | Only PENDING can be claimed | `TestProcessBatch_SkipsSuccess` |
| Submit timeout | `WithSubmitTimeout` wraps the ctx per request | `TestProcessDocument_SubmitTimeout` |
| No goroutine leaks | Fixed-size worker pool, every goroutine is waited on | `goleak.VerifyTestMain` + `synctest` (fails if a goroutine is left behind) |
| No lock held during Submit | The mutex lives only inside the repository; DB transactions finish before Submit | code review of `ProcessDocument` |
| Error wrapping and sentinels | `ErrBatchNotFound`, `ErrDocumentNotFound`, `ErrInvalidInput`, `ErrInvalidCallback`, `ErrBatchConflict`, `ErrNotClaimable`, `ErrStaleAttempt`, `ErrSubmitFailed`, all wrapped with `%w` | `TestNotFoundErrors`, `TestHandleCallback_Validation`, `TestCreateBatch_Conflict` |

Optional items done: PostgreSQL repository + integration tests, HTTP callback
endpoint, exponential backoff with jitter, graceful shutdown + lease-based
recovery of stuck documents, simple metrics (`/metrics`) and structured
logging (`log/slog`).

## Why the tests are not flaky

The task asks for deterministic tests that do not rely on `time.Sleep` and
pass with `-race -count=10`. There is not a single `time.Sleep` in the tests.
How:

1. **`testing/synctest` for tests that depend on "when".** The concurrency
   limit test holds every Submit on a channel (the gate), then calls
   `synctest.Wait()`. That only returns once **every** goroutine in the
   bubble is blocked. At that moment the number of running Submits is final:
   if the limiter leaked it would be 20, not 3. There is no timing window
   that can be missed on a slow CI machine. "Sleep 100ms then check" passes
   on a laptop and fails on a busy CI runner.
2. **Virtual time for timeouts.** Inside synctest a 30-second
   `context.WithTimeout` fires instantly and always exactly, because the
   bubble's clock only advances when every goroutine is blocked.
3. **A manual clock for backoff and leases.** `WithClock` injects a clock
   that only moves through `clock.Advance(...)`.
4. **Barriers for real races.** The "64 goroutines call ProcessDocument on
   the same ID" test uses `close(start)` to release them all at once, then
   checks the **end result** (exactly 1 Submit), not the execution order.
5. **Invariants, not order.** `TestMixedOperations_NoRace` runs every
   operation at once with a genuinely random schedule, and only checks what
   must hold in any order: Submits = `attempt_count`, attempts ≤ 3, one
   record per callback, per-status totals = total documents.
6. **Deterministic mocks.** Failures are decided by the document's ID and
   attempt, not by `rand`.

I also made sure the tests can actually fail (mutation checks): I
deliberately broke the limiter (buffer of 100), removed the status check from
the claim, let FAILED overwrite SUCCESS, disabled callback dedupe, used the
already-cancelled ctx for the release, removed the attempt limit, and
disabled the CreateBatch conflict check. Each break was caught right away by
the relevant test. The same goes for the PostgreSQL claim SQL.

## Design decisions

Summary here; the full explanation with diagrams is in
[docs/design.md](docs/design.md).

- **The semaphore is taken before the claim**, not after. Documents waiting
  for a slot stay PENDING, so a cancel never leaves behind a PROCESSING
  document that was never sent. The limit of 3 also applies to
  `ProcessDocument` called directly.
- **`ProcessBatch` uses a fixed-size worker pool** (≤ 3 goroutines per
  call), not one goroutine per document. A batch of 100k documents does not
  create 100k goroutines.
- **A failed Submit is not a batch-level error.** The document is recorded as
  FAILED and can be retried; `ProcessBatch` only returns `ctx.Err()` or
  infrastructure errors (DB down). `ProcessDocument` returns a wrapped
  `ErrSubmitFailed` so direct callers still know.
- **The Submit result is saved with a separate ctx** (`context.WithoutCancel`
  + a 5-second timeout). If the caller's ctx is cancelled right after Submit
  succeeds, the result must still be recorded.
- **Fencing by attempt.** Every update after Submit uses
  `WHERE status = 'PROCESSING' AND attempt_count = <claimed attempt>`. A slow
  worker whose document was taken over by recovery cannot overwrite
  anything. The repository reports that case as `ErrStaleAttempt`, so the
  service can tell "a callback already settled this document" apart from a
  real failure, and does not count or log it as one.
- **`submitted_at` separates "worker died" from "waiting for the callback".**
  Recovery only takes the former.
- **A SUCCESS callback is accepted from PENDING/PROCESSING/FAILED**, while
  **FAILED only applies to PROCESSING**. See Assumptions.
- **RetryFailed does not reprocess right away.** It only moves FAILED →
  PENDING and returns quickly; the caller decides when `ProcessBatch` runs.
- **Batch status is derived from the documents**, not stored in
  `batches.status` on every change. Updating one batch row for every
  document change would make it a hot row every worker fights over.
- **GORM for CRUD, explicit SQL for the queries concurrency depends on**
  (claim, recovery, `FILTER` aggregation). Critical queries are easier to
  review as plain SQL, and they match `db/queries.sql` exactly. The schema
  does not use `AutoMigrate`: `db/schema.sql` is the single source of truth,
  including CHECK constraints and partial indexes.
- **Contract tests for Repository.** The memory and postgres implementations
  run the same suite, so Service unit tests on top of memory stay relevant
  for production behaviour.
- **`net/http` instead of a web framework.** Go 1.22+ routing already
  supports method and path patterns (`POST /v1/batches/{batchNo}/process`),
  which is all this service needs. The business logic does not depend on
  the HTTP layer, so moving to Echo, Gin or Chi only means rewriting
  `httpapi`.
- **No mutable global variables.** All state lives in `Service` and the
  repositories. The only package-level variables are sentinel errors (a
  standard Go idiom, never reassigned); the list of statuses is provided by
  the `Statuses()` function precisely so there is no global slice/array that
  could be modified.

## Assumptions

- **`CreateBatch` with an existing `batchNo` is only idempotent if the
  content is the same.** A client retrying after a timeout gets the existing
  batch with no error. If the content differs (number of documents,
  `document_no`, or payload) the result is `ErrBatchConflict` (HTTP 409),
  because the `batch_no` was most likely reused by mistake. Payloads are
  compared as JSON values, not bytes, because JSONB rewrites them with
  different key order and whitespace. Document order is ignored.
- **A SUCCESS callback for a PENDING document is accepted.** The case:
  attempt 1 times out although the Principal actually accepted it,
  `RetryFailed` moves the document back to PENDING, then the SUCCESS callback
  for attempt 1 arrives. The Principal already succeeded, so resending would
  only produce a duplicate. This goes slightly beyond the state machine table
  in the task, on purpose.
- **A FAILED callback for a PENDING or FAILED document is ignored** (still
  recorded for auditing). Callbacks do not carry an attempt number, so a late
  FAILED from attempt 1 cannot be told apart from a FAILED for attempt 2 while
  attempt 2 is PROCESSING. That is a limit of the contract; ideally the
  callback would carry the `request_id` we sent.
- **A callback with the same `callback_id` but a different `document_id`**
  is treated as a duplicate (no-op), because `callback_id` is unique per event
  from the Principal.
- **Cancelling in the middle of Submit still counts the attempt.** The
  request may already have reached the Principal. Giving the attempt back
  could exceed the "at most 3 Submits" limit. A document cancelled on its
  last attempt becomes FAILED directly, not a PENDING that could never be
  claimed.
- Document payloads must be valid JSON (the column is JSONB).

## Test results

### Automated tests

Run on 25 November 2026 on macOS (Apple Silicon), Go 1.27, PostgreSQL 17 in a
container (OrbStack):

```
$ TEST_DATABASE_URL=postgres://...docproc_test go test -race -count=10 ./...
ok   github.com/sabiqsa/go-document-processor/cmd/docproc-server  1.968s
ok   github.com/sabiqsa/go-document-processor/docproc            1.628s
ok   github.com/sabiqsa/go-document-processor/docproc/memory     2.434s
ok   github.com/sabiqsa/go-document-processor/docproc/postgres   8.740s
ok   github.com/sabiqsa/go-document-processor/httpapi           11.743s
ok   github.com/sabiqsa/go-document-processor/principal          4.253s
```

These automated tests are the main evidence for every requirement (see the
mapping table above). The full steps, including mutation checks, are in
[docs/testing.md](docs/testing.md).

### Manual HTTP tests

The server ran in PostgreSQL mode with the simulated Principal and was called
from Bruno and `curl`. Table contents were checked in DBeaver.

| # | Step | Result |
|---|---|---|
| 1 | `POST /v1/batches` with 3 documents | `200`, all three documents `PENDING` |
| 2 | `POST /v1/batches/INV-001/process` | `202 processing started` |
| 3 | `GET /v1/batches/INV-001` | 2 `SUCCESS`, 1 `FAILED`, `retryable: 1`, state `IN_PROGRESS` |
| 4 | `POST /v1/batches/INV-001/retry` after the backoff | `retried: 1` |
| 5 | `documents` table | D2 back to `PENDING`. `attempt_count` stays 1 and `last_error` is kept, so the next run is attempt 2 |
| 6 | `POST /v1/callbacks` FAILED for D1, which is already SUCCESS, sent twice | `200 accepted` |
| 7 | Join of `documents` and `document_callbacks` | D1 stays `SUCCESS`. The FAILED callback is recorded for auditing, as **1 row** although it was sent twice |
| 8 | `GET /metrics` | `CallbacksIgnored: 1` (the late FAILED), `CallbacksDuplicate: 2`, `InFlight: 0` |
| 9 | `POST /v1/batches` with an empty `batch_no` | `422 invalid input: batch_no is required` |
| 10 | Callback for a `document_id` that does not exist | `404 document not found` |
| 11 | Callback with status `DONE` | `422 status must be SUCCESS or FAILED` |
| 12 | Callback with `Content-Type: text/plain` | `415 content-type must be application/json` |
| 13 | Batch of 30 documents, `/process` called twice back to back | `202`, then `409 batch is already being processed` |
| 14 | Retry and process again until done | `retried: 0` (nothing left to retry), state `COMPLETED`, 3 `SUCCESS` |
| 15 | `POST /v1/batches` again with the same `batch_no` and the same documents | `200`, the existing batch is returned, no new batch |
| 16 | `POST /v1/batches` with the same `batch_no` but different documents (`X`) | `409 batch_no already used with different documents` |

<details>
<summary>Screenshots</summary>

**1. Creating a batch**
![Creating a batch](docs/images/manual-test/01-create-batch.png)

**2. Processing the batch**
![Processing the batch](docs/images/manual-test/02-process-batch.png)

**3. Batch status after processing**
![Batch status](docs/images/manual-test/03-batch-status.png)

**4. Retrying the failed document**
![Retry](docs/images/manual-test/04-retry-failed.png)

**5. documents table after the retry**
![documents table](docs/images/manual-test/05-db-after-retry.png)

**6. A late FAILED callback**
![Late FAILED callback](docs/images/manual-test/06-callback-failed-late.png)

**7. D1 stays SUCCESS, the callback is recorded once**
![Callback not applied](docs/images/manual-test/07-db-callback-not-applied.png)

**8. Metrics**
![Metrics](docs/images/manual-test/08-metrics.png)

**9. CreateBatch validation (422)**
![422 batch](docs/images/manual-test/09-create-batch-422.png)

**10. Callback for a document that does not exist (404)**
![404 callback](docs/images/manual-test/10-callback-404.png)

**11. Callback with an invalid status (422)**
![422 callback](docs/images/manual-test/11-callback-422.png)

**12. Wrong Content-Type (415)**
![415 callback](docs/images/manual-test/12-callback-415.png)

**13. Process while the batch is still running (409)**
![409 process](docs/images/manual-test/13-process-409.png)

**14. Retry until the batch is COMPLETED**
![Batch completed](docs/images/manual-test/14-retry-completed.png)

**15. CreateBatch again with the same content (idempotent)**
![CreateBatch idempotent](docs/images/manual-test/15-create-batch-idempotent.png)

**16. CreateBatch with different content (409)**
![CreateBatch conflict](docs/images/manual-test/16-create-batch-conflict.png)

</details>

## Benchmark results

Apple Silicon MacBook (M-series), Go 1.27, in-memory repository, client with
no latency. This measures the service overhead (claim + semaphore + update),
not throughput to the Principal, which is dominated by network latency:

```
BenchmarkProcessBatch/docs=10-10      27.8 µs/op     8 KB/op     139 allocs/op
BenchmarkProcessBatch/docs=100-10    133.5 µs/op    74 KB/op    1227 allocs/op
BenchmarkProcessBatch/docs=1000-10   890.6 µs/op   769 KB/op   12041 allocs/op
```

About 0.9 µs and 12 allocations per document, linear in the batch size. With
200 ms of Principal latency and a limit of 3 concurrent requests, the
theoretical throughput is 15 documents/second per instance. That limit
decides throughput, not this code.

## What I would improve with more time

1. **A `request_id` that is stable across recovery** as the
   `Idempotency-Key`, replacing the per-attempt key (discussed in section C
   of `docs/answers.md`). This is the most important improvement for
   preventing duplicates at the Principal after a crash.
2. **A reconciliation job**: documents with `submitted_at` that get no
   callback within X hours are checked against the Principal. Needs a status
   endpoint on the Principal side.
3. **A concurrency limit across instances.** Today the limit of 3 is per
   instance (as the task specifies). If the Principal limits 3 in total
   across every instance, this needs a distributed semaphore (e.g. a
   PostgreSQL advisory lock with 3 slots, or Redis).
4. **Chunked claims with `SKIP LOCKED`** (query #5) for the PostgreSQL
   version of `ProcessBatch`, replacing "list IDs, then claim one by one".
   Fewer round trips and more efficient when many instances process the same
   batch.
5. **Testcontainers** for the integration tests. They already run against a
   real PostgreSQL through `make db-up`; Testcontainers would remove that
   manual step.
6. **Prometheus metrics** (Submit latency histogram, in-flight gauge,
   per-status counters) and OpenTelemetry tracing, replacing the simple
   counters.
7. **Versioned schema migrations** (goose/atlas) instead of a single
   `schema.sql` file.
