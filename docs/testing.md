# Testing guide

Steps to verify every requirement from scratch, from the automated tests to
trying the server with `curl`. Every sample output below comes from actually
running the step.

Contents:

1. [Setup](#1-setup)
2. [Automated tests without a database](#2-automated-tests-without-a-database)
3. [PostgreSQL integration tests](#3-postgresql-integration-tests)
4. [Trying the server by hand](#4-trying-the-server-by-hand)
5. [Proving the tests can fail](#5-proving-the-tests-can-fail-mutation-checks)
6. [Requirements mapped to tests](#6-requirements-mapped-to-tests)
7. [Troubleshooting](#7-troubleshooting)
8. [Cleaning up](#8-cleaning-up)

---

## 1. Setup

| Requirement | Check | Notes |
|---|---|---|
| Go 1.25+ | `go version` | Needs `testing/synctest` and `sync.WaitGroup.Go` |
| Docker | `docker version` | OrbStack, Docker Desktop, or Colima. Only for sections 3 and 4b |
| curl, openssl | included with macOS | For section 4 |

Run every command from the repository root:

```bash
cd go-document-processor
```

## 2. Automated tests without a database

This covers the hard requirement of the task: the code builds and passes the
tests with the race detector.

```bash
go build ./... && go vet ./...
```

```bash
go test -race -count=10 ./...
```

`-count=10` runs every test 10 times. Flaky concurrency tests usually show up
here. Expected result:

```
ok   github.com/sabiqsa/go-document-processor/cmd/docproc-server  ...
ok   github.com/sabiqsa/go-document-processor/docproc            ...
ok   github.com/sabiqsa/go-document-processor/docproc/memory     ...
ok   github.com/sabiqsa/go-document-processor/docproc/postgres   ...   <- its tests are skipped
ok   github.com/sabiqsa/go-document-processor/httpapi            ...
ok   github.com/sabiqsa/go-document-processor/principal          ...
```

The `docproc/postgres` package still says `ok`, but the tests inside it are
skipped because `TEST_DATABASE_URL` is empty. Section 3 runs them.

Other useful commands:

```bash
# Show every test name and its result
go test -race -v ./docproc/ 2>&1 | grep -E '^(--- |ok|FAIL)'

# Run a single test
go test -race -run TestHandleCallback_LateFailedKeepsSuccess -v ./docproc/

# Benchmark (about 0.9 µs and 12 allocations per document)
make bench

# Coverage
go test -coverprofile=cover.out ./... && go tool cover -func=cover.out | tail -1

# Make sure no test uses time.Sleep (the output must be empty)
grep -rn "time.Sleep" --include='*_test.go' .
```

## 3. PostgreSQL integration tests

```bash
make db-up
```

This starts `postgres:17-alpine` and waits until the container is `Healthy`.
While the volume is still empty, the container creates two databases:

- `docproc` with the schema from `db/schema.sql`, for running the server.
- `docproc_test`, empty. The integration tests drop and recreate its tables
  on every run.

```bash
make test-integration
```

Check that the Postgres tests really ran rather than being skipped:

```bash
TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5432/docproc_test?sslmode=disable" \
  go test -count=1 -v ./docproc/postgres/ 2>&1 | grep -E '^--- '
```

Expected (must be `PASS`, not `SKIP`):

```
--- PASS: TestQueriesSQL
--- PASS: TestRepositoryContract
--- PASS: TestCreateBatchConflictOnPostgres
--- PASS: TestServiceOnPostgres
```

| Test | What it proves |
|---|---|
| `TestRepositoryContract` | The same 11 scenarios as the memory repository: atomic claim, fencing, parallel callbacks, recovery, and so on |
| `TestCreateBatchConflictOnPostgres` | JSONB's rewritten payloads do not cause false CreateBatch conflicts, while real differences are rejected |
| `TestServiceOnPostgres` | The full Service flow on PostgreSQL |
| `TestQueriesSQL` | Every statement in `db/queries.sql` is `PREPARE`d against the schema, so a typo in a column name is caught right away |

## 4. Trying the server by hand

### 4a. Memory mode (no database)

With no environment variables, the server uses the memory repository and the
simulated Principal. The simulator misbehaves on purpose: roughly 15% of
Submits are rejected, 20% of callbacks report FAILED, and 20% of callbacks
are sent twice.

```bash
make run
```

Keep this terminal open to watch the logs. Run the following commands in
another terminal.

### 4b. PostgreSQL mode

In this mode the table contents can be inspected directly.

```bash
make db-up
DATABASE_URL="postgres://postgres:postgres@localhost:5432/docproc?sslmode=disable" make run
```

To look at the table:

```bash
docker compose exec postgres psql -U postgres -d docproc -c \
  "select id, document_no, status, attempt_count, last_error from documents order by id"
```

### Scenarios

The simulator is random, so the number of SUCCESS and FAILED documents you
see may differ from the samples below.

**1. Create a batch**

```bash
curl -s -X POST localhost:8080/v1/batches -H 'Content-Type: application/json' -d '{
  "batch_no": "INV-001",
  "documents": [
    {"document_no": "D1", "payload": {"amount": 1000}},
    {"document_no": "D2", "payload": {"amount": 2500}},
    {"document_no": "D3", "payload": {"amount": 700}},
    {"document_no": "D4", "payload": {"amount": 120}},
    {"document_no": "D5", "payload": {"amount": 9000}}
  ]}'
```

```json
{"id":"1","batch_no":"INV-001","documents":[{"id":"1","document_no":"D1","status":"PENDING"}, ...]}
```

Note the document `id`s; the callback scenarios use them. In memory mode,
batches and documents share one ID counter, so the first document may have
ID `2`.

**2. CreateBatch is idempotent.** Run exactly the same command again. The
response is the same batch with the same ID, and no duplicate is created.

If the `batch_no` is the same but the documents differ, the result is `409`:

```bash
curl -s -w ' [%{http_code}]\n' -X POST 'http://localhost:8080/v1/batches' \
  -H 'Content-Type: application/json' \
  -d '{"batch_no":"INV-001","documents":[{"document_no":"X","payload":{}}]}'
```

```
{"error":"create batch \"INV-001\": batch_no already used with different documents"} [409]
```

**3. Input validation returns 422**

```bash
curl -s -w ' [%{http_code}]\n' -X POST localhost:8080/v1/batches \
  -H 'Content-Type: application/json' -d '{"batch_no":"","documents":[]}'
```

```
{"error":"invalid input: batch_no is required"} [422]
```

**4. Process the batch**

```bash
curl -s -w ' [%{http_code}]\n' -X POST localhost:8080/v1/batches/INV-001/process
```

```
{"result":"processing started"} [202]
```

Running it again while the batch is still being processed returns
`409 batch is already being processed`. Once the batch has finished, it
returns 202 again and does nothing, because nothing is PENDING any more.

To see the 409 reliably, create a batch of about 30 documents (say
`INV-004`, the same way as step 1) and send both requests in one `curl`
call, so there is no gap between them (the simulator finishes 30 documents
in well under a second, faster than you can click):

```bash
curl -s -w ' [%{http_code}]\n' -X POST 'http://localhost:8080/v1/batches/INV-004/process' 'http://localhost:8080/v1/batches/INV-004/process'
```

**5. Check the status**

```bash
curl -s localhost:8080/v1/batches/INV-001
```

```json
{"batch_no":"INV-001","counts":{"FAILED":3,"PENDING":0,"PROCESSING":0,"SUCCESS":2},
 "done":false,"retryable":3,"state":"IN_PROGRESS","total":5}
```

- `retryable`: the number of FAILED documents whose `attempt_count` is still
  below 3.
- `done`: `true` when no document can change status any more.

**6. Retry and backoff**

```bash
curl -s -X POST localhost:8080/v1/batches/INV-001/retry
```

Run right after the documents failed, it returns:

```json
{"retried":0}
```

That is not a bug. The server uses exponential backoff of 5–10 seconds for
the first attempt, so a document may only be retried once its
`next_retry_at` has passed. Wait about 10 seconds, then:

```bash
curl -s -X POST localhost:8080/v1/batches/INV-001/retry          # {"retried":3}
curl -s -X POST localhost:8080/v1/batches/INV-001/process
curl -s localhost:8080/v1/batches/INV-001
```

Repeat until `"state":"COMPLETED"`. A batch is complete when every document
is SUCCESS, or the FAILED ones have used all 3 attempts. After that, `retry`
always returns `{"retried":0}`, because SUCCESS documents are never processed
again and documents out of attempts are not retried.

**7. Callbacks**

The examples use the `document_id` of a document that is already SUCCESS.
Look up its ID in the table or in the create response first.

```bash
cb() { curl -s -w " [%{http_code}]\n" -X POST localhost:8080/v1/callbacks \
         -H 'Content-Type: application/json' -d "$1"; }
```

| Scenario | Command | Result |
|---|---|---|
| A late FAILED for a SUCCESS document | `cb '{"callback_id":"manual-1","document_id":"2","status":"FAILED"}'` | `200`, status stays SUCCESS |
| The same callback again | repeat the command above | `200`, counted as a duplicate |
| Unknown document | `cb '{"callback_id":"manual-2","document_id":"999","status":"SUCCESS"}'` | `404` |
| Invalid status | `cb '{"callback_id":"manual-3","document_id":"2","status":"DONE"}'` | `422` |
| Malformed JSON | `cb '{"callback_id":'` | `400` |
| Wrong Content-Type | `curl -s -w ' [%{http_code}]\n' -X POST localhost:8080/v1/callbacks -H 'Content-Type: text/plain' -d x` | `415` |

The late FAILED callback is still stored in `document_callbacks` for
auditing, but does not change the document status. The server log shows:

```
"msg":"callback recorded but not applied","callback_status":"FAILED","current_status":"SUCCESS"
```

To check it in PostgreSQL mode:

```bash
docker compose exec postgres psql -U postgres -d docproc -c \
  "select d.id, d.status, c.callback_id, c.status as cb_status
     from documents d join document_callbacks c on c.document_id = d.id
    where c.callback_id like 'manual%'"
```

**8. HMAC signature**

Start the server with a secret:

```bash
CALLBACK_SECRET=local-secret make run
```

```bash
BODY='{"callback_id":"hmac-1","document_id":"2","status":"SUCCESS"}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "local-secret" | awk '{print $NF}')

# no signature -> 401
curl -s -w ' [%{http_code}]\n' -X POST localhost:8080/v1/callbacks \
  -H 'Content-Type: application/json' -d "$BODY"

# with signature -> processed (200, or 404 if the document_id does not exist)
curl -s -w ' [%{http_code}]\n' -X POST localhost:8080/v1/callbacks \
  -H 'Content-Type: application/json' -H "X-Principal-Signature: sha256=$SIG" -d "$BODY"
```

The signature covers the body exactly as sent. If even one space in `BODY`
changes, `SIG` must be recomputed.

Without `CALLBACK_SECRET` the server logs a warning and accepts unsigned
callbacks, which is only allowed with the simulator. Setting `PRINCIPAL_URL`
without `CALLBACK_SECRET` makes the server refuse to start:

```bash
PRINCIPAL_URL=https://principal.example make run
```

```
"msg":"server stopped with error","err":"CALLBACK_SECRET is required when PRINCIPAL_URL is set"
```

**9. Metrics**

```bash
curl -s localhost:8080/metrics
```

```json
{"SubmitCalls":10,"SubmitFailures":0,"InFlight":0,"CallbacksApplied":10,
 "CallbacksIgnored":1,"CallbacksDuplicate":3,"Recovered":0}
```

| Field | Meaning |
|---|---|
| `SubmitCalls` / `SubmitFailures` | Requests to the Principal, and how many failed and were recorded as FAILED (a failure that arrives after a callback already settled the document is not counted) |
| `InFlight` | Submits running right now. Never more than 3 |
| `CallbacksApplied` | Callbacks that changed the status |
| `CallbacksIgnored` | Callbacks recorded but not applied, e.g. FAILED for a SUCCESS document |
| `CallbacksDuplicate` | `callback_id`s that had already been received |
| `Recovered` | Stuck documents taken over by `RecoverStale` |

**10. Graceful shutdown**

Press Ctrl+C in the server terminal. The last log lines:

```
"msg":"shutdown signal received"
"msg":"shutdown complete","stats":{...}
```

To try a shutdown in the middle of processing, create a batch of 30
documents, call `/process`, and press Ctrl+C right away. Then check in
PostgreSQL mode:

```bash
docker compose exec postgres psql -U postgres -d docproc -c \
  "select status, submitted_at is not null as submitted, count(*)
     from documents group by 1, 2 order by 1, 2"
```

There must be **no** `PROCESSING | f` row, i.e. a document that was claimed
but never accepted by the Principal. Such documents are returned to PENDING
on shutdown, or taken by `RecoverStale` once the lease expires.

`PROCESSING | t` rows are fine. They mean the Principal accepted the document
and the system is waiting for the callback. In this experiment the callback
never comes, because the simulator stopped together with the server. A real
Principal would still send it. Lost callbacks are covered in the README's
improvement list (reconciliation).

## 5. Proving the tests can fail (mutation checks)

A test that always passes does not necessarily test anything. The way to
prove it: break the code on purpose and check that the test fails. Two
examples to try. Revert the change afterwards.

**Example A: let FAILED overwrite SUCCESS.** In
`docproc/memory/repository.go`, change the line

```go
case cb.Status == docproc.StatusFailed && d.doc.Status == docproc.StatusProcessing:
```

to

```go
case cb.Status == docproc.StatusFailed:
```

```bash
go test -run TestHandleCallback_LateFailedKeepsSuccess ./docproc/
```

```
--- FAIL: TestHandleCallback_LateFailedKeepsSuccess
    callback_test.go:92: doc = FAILED (err late), want SUCCESS with no error
```

**Example B: enlarge the semaphore.** In `docproc/service.go`, change
`make(chan struct{}, MaxConcurrent)` to
`make(chan struct{}, MaxConcurrent+2)`.

```bash
go test -run 'TestProcessBatch_MaxConcurrentSubmit|TestConcurrencyLimitIsGlobalAcrossBatches' -v ./docproc/
```

`TestProcessBatch_MaxConcurrentSubmit` **still passes**, because a single
`ProcessBatch` only ever runs 3 workers. The one that fails is
`TestConcurrencyLimitIsGlobalAcrossBatches`:

```
concurrency_test.go:75: in-flight across batches = 5, want 3
```

This is why two tests are needed. The first only proves one batch never
exceeds 3. The second proves the limit really is global, across every batch
running at the same time.

Revert with `git checkout -- docproc/`.

## 6. Requirements mapped to tests

| Requirement | Test | Package |
|---|---|---|
| Max 3 concurrent requests | `TestProcessBatch_MaxConcurrentSubmit`, `TestConcurrencyLimitIsGlobalAcrossBatches` | `docproc` |
| Atomic claim, a document is never processed twice | `TestProcessDocument_SameIDManyGoroutines`, contract `ClaimIsAtomic` | `docproc`, repo |
| Max 3 attempts | `TestRetry_StopsAtMaxAttempts`, `TestRetry_RecoversBeforeLimit` | `docproc` |
| SUCCESS is not processed again | `TestProcessBatch_SkipsSuccess` | `docproc` |
| Idempotent callbacks | `TestHandleCallback_Duplicate`, contract `CallbackIdempotentUnderConcurrency` | `docproc`, repo |
| A late FAILED never overwrites SUCCESS | `TestHandleCallback_LateFailedKeepsSuccess`, `TestHandleCallback_SuccessAndFailedRace` | `docproc` |
| A callback that settles the document during Submit wins | `TestProcessDocument_CallbackSettlesDuringSubmit`, contract `FencingByAttempt` | `docproc`, repo |
| `context.Context` is respected | `TestProcessBatch_ContextCancelled`, `TestProcessBatch_AlreadyCancelledContext` | `docproc` |
| Submit timeout | `TestProcessDocument_SubmitTimeout` | `docproc` |
| Passes `-race` | `TestMixedOperations_NoRace` and `go test -race -count=10` | all |
| Backoff + jitter | `TestBackoff_DelaysRetry`, `TestExponentialBackoff` | `docproc` |
| Lease recovery | `TestRecoverStale`, contract `RecoverStale` | `docproc`, repo |
| CreateBatch idempotent, different content rejected | `TestCreateBatch_Idempotent`, `TestCreateBatch_Conflict`, `TestCreateBatchConflictOnPostgres`, `TestBatchEndpoints` | `docproc`, `docproc/postgres`, `httpapi` |
| HTTP callback and status codes | `TestCallbackEndpoint`, `TestCallbackEndpoint_MethodNotAllowed`, `TestCreateBatch_BodyTooLarge` | `httpapi` |
| Safe configuration (secret required with a real Principal) | `TestLoadConfig` | `cmd/docproc-server` |
| SQL queries are valid | `TestQueriesSQL` | `docproc/postgres` |
| No goroutine leaks | `TestMain` with `goleak` | `docproc` |

## 7. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `make db-up` fails, port 5432 already in use | Another PostgreSQL is running, e.g. from Homebrew. Stop it with `brew services stop postgresql@17` |
| `bind: address already in use` on `make run` | An old server is still running. Find it with `lsof -i :8080` and stop it |
| `docker: command not found` or `Cannot connect to the Docker daemon` | Open OrbStack or Docker Desktop first and wait until it is running |
| Postgres tests show `SKIP` | `TEST_DATABASE_URL` was not picked up. Use `make test-integration`, or set the variable on the same command line |
| Tables missing in `docproc`, or an old schema | Init scripts only run on an empty volume. Reset with `make db-down`, then `make db-up` |
| `go: go.mod requires go >= 1.25.0` | Go is too old. Update with `brew upgrade go` |
| `retry` always returns `{"retried":0}` | The backoff has not passed yet (wait 10 seconds or more), or the FAILED documents already used 3 attempts |
| Code changes have no effect on the running server | `make run` compiles once at startup. Stop the server with Ctrl+C and start it again |
| A pasted command turns `;` into `\;` and fails with `404` or a `for>` prompt | zsh's `url-quote-magic` escapes `;` right after a URL. Put the URL in quotes, avoid `;` after it, or run `zle -A .self-insert self-insert` in that terminal tab first |

## 8. Cleaning up

```bash
docker compose stop        # stop the container, keep the data
make db-down               # stop the container AND delete its data (docker compose down -v)
rm -f cover.out
```

After `make db-down`, the next `make db-up` starts from an empty database and
applies `db/schema.sql` again.
