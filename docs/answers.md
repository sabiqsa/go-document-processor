# Essay answers: transactions and performance

Every example below comes from the system in this repository (the
`documents` and `document_callbacks` tables, the `ProcessDocument` function,
and so on). The `EXPLAIN` output in section D is real output from a local
PostgreSQL 17 with about 600k test documents, not made up.

---

## A. The risk of SELECT then UPDATE without locking

Suppose the document claim were written in a way that looks reasonable:

```go
doc := repo.Find(id)                  // SELECT * FROM documents WHERE id = 42
if doc.Status == "PENDING" {
    doc.Status = "PROCESSING"
    doc.AttemptCount++                // computed in the application
    repo.Save(doc)                    // UPDATE documents SET status=..., attempt_count=1 WHERE id = 42
    principal.Submit(doc)
}
```

With two workers (or two service instances) running at the same time:

| Time | Worker A | Worker B |
|---|---|---|
| t1 | `SELECT` id 42 → PENDING, attempt 0 | |
| t2 | | `SELECT` id 42 → PENDING, attempt 0 |
| t3 | `UPDATE` → PROCESSING, attempt 1 | |
| t4 | | `UPDATE` → PROCESSING, attempt 1 (overwrites) |
| t5 | `Submit(42)` | `Submit(42)` |

At t2, B still sees PENDING because A has not committed yet, or has
committed but B read first. The `if` check runs in the application on stale
data, so both pass. Wrapping the code in `BEGIN ... COMMIT` does not help
either: at the default isolation level (READ COMMITTED), a plain `SELECT`
locks nothing.

The consequences:

1. **The document is sent to the Principal twice.** If the document is an
   invoice or a payment instruction, the Principal may process it twice.
   That is a business problem, not just a technical one.
2. **The attempt count is wrong (lost update).** There are two Submits, but
   `attempt_count` is only 1 because B overwrote A's value. The "at most 3
   attempts" limit breaks: the document can be sent 4–6 times before it is
   considered exhausted.
3. **Two callbacks for one document.** The Principal sends two callbacks with
   different CallbackIDs, say one SUCCESS and one FAILED. Callback dedupe does
   not help because the IDs really are different; only the rule "FAILED never
   overwrites SUCCESS" saves the day.
4. Quota / rate limit at the Principal is wasted on duplicate requests.

Mitigations (the ones used in this repository are marked **[used]**):

- **[used]** **An atomic conditional UPDATE (compare-and-set).** The check
  moves from the application's `if` into the `WHERE` clause:
  `UPDATE ... SET status='PROCESSING', attempt_count = attempt_count + 1
  WHERE id = $1 AND status = 'PENDING' RETURNING *`. If B runs this while A
  holds the row, B waits. After A commits, PostgreSQL re-evaluates the
  `WHERE` against the latest row version, finds the status is already
  PROCESSING, and updates 0 rows. B learns it lost from `RowsAffected = 0`.
  `attempt_count + 1` is computed by the database, so there is no lost
  update.
- **[used]** **`SELECT ... FOR UPDATE SKIP LOCKED`.** The row is locked when
  read. `SKIP LOCKED` makes other workers skip the row instead of waiting,
  which suits a work queue (query #5). `q5b_claim_one_document` combines both:
  a `FOR UPDATE SKIP LOCKED` subquery inside the conditional UPDATE.
- **Optimistic locking** with a `version` column:
  `UPDATE ... WHERE id = $1 AND version = $2`. Same effect as
  compare-and-set; useful when the update cannot be expressed as a single
  status condition.
- **SERIALIZABLE isolation.** PostgreSQL fails one of the transactions with
  error `40001`, but the application must be ready to retry. Too heavy for a
  case that one conditional UPDATE already solves.
- **[used]** **A last line of defence at the Principal:** the
  `Idempotency-Key` header (see section C). It does not prevent the race, it
  only limits the damage if a race slips through.

In the in-memory implementation the mechanism is a mutex: checking and
changing the status happen inside one `mu.Lock()`. The test
`TestProcessDocument_SameIDManyGoroutines` runs 64 goroutines at once for the
same document and checks that Submit happens exactly once. The contract test
`ClaimIsAtomic` runs the same scenario against PostgreSQL.

---

## B. Transaction vs row-level lock vs unique constraint

The three are often treated as interchangeable, but each answers a different
question.

| | Problem it solves | When it is not enough |
|---|---|---|
| **Transaction** | Atomicity: several changes become all-or-nothing. Examples: insert callback + update document; insert batch + 250 documents. | It does not stop two transactions from reading the same row and acting on that read (the case in section A). It cannot roll back effects outside the database: an HTTP request already sent to the Principal is not undone. A long transaction (e.g. one wrapping Submit) holds locks and a connection during network I/O. |
| **Row-level lock** (`FOR UPDATE`, or the implicit lock from `UPDATE`) | Serialises writers that touch an **existing row**. Example: two callbacks for document 42 are processed one after the other, not at the same time. | It cannot lock a row that **does not exist yet**: two transactions that both check "does callback X exist?" both get "no", then both insert. Locks are released at commit, so they cannot "hold" a document during a Submit that may take seconds. Inconsistent lock ordering can cause deadlocks. |
| **Unique constraint** | Guarantees no duplicates, **including** concurrent inserts, and applies to every code path (application, manual scripts, migrations). | It only guards existence, not status transitions: it does not stop FAILED from overwriting SUCCESS. It is also only as good as the key; if the Principal resends the same event with a new CallbackID, the constraint cannot recognise it. |

In this design all three (plus one more tool: conditions in the `WHERE`
clause) work together:

**Document claim (`ProcessDocument`)**

- Row lock: `FOR UPDATE SKIP LOCKED` in the claim subquery.
- WHERE condition: `status = 'PENDING' AND attempt_count < max_attempts`.
- Transaction: no explicit one needed; a single UPDATE statement is atomic.
- Because no lock may be held during Submit, "ownership" of the document is
  represented by **status PROCESSING + `locked_at`** (a logical lease), not a
  database lock. After Submit, the result is saved with the fence
  `WHERE status = 'PROCESSING' AND attempt_count = <claimed attempt>`, so a
  worker whose document has been taken over cannot overwrite anything. The
  repository reports that case as `ErrStaleAttempt`.
- The CHECK constraint `attempt_count BETWEEN 0 AND max_attempts` is a safety
  net in case of an application bug.

**Callback (`HandleCallback`)** in a single transaction:

1. `SELECT ... FOR UPDATE` on the document → row lock. Every callback for the
   same document is serialised, and a missing document is detected right
   away (404).
2. `INSERT ... ON CONFLICT (callback_id) DO NOTHING RETURNING id` → unique
   constraint. For identical callbacks arriving together, the second
   transaction waits on the unique index until the first commits, then gets
   0 rows = duplicate.
3. `UPDATE ... WHERE id = $1 AND status = 'PROCESSING'` (for FAILED) → the
   WHERE condition guards the transition, so a late FAILED cannot overwrite
   SUCCESS.
4. The transaction keeps steps 2 and 3 together. There must never be a state
   of "callback recorded but status unchanged" because the process died
   between them; if that happened, the Principal's retry of the callback
   would be seen as a duplicate and the status would never be updated.

**CreateBatch**: a unique `batch_no` + `ON CONFLICT DO NOTHING` makes a
repeated call (a client retrying after a timeout) return the same batch, and
the transaction guarantees no batch is stored without its documents. A
repeated call with different documents is rejected with `ErrBatchConflict`,
so a reused `batch_no` does not slip through silently.

---

## C. The Principal succeeded, but the database update failed

The concrete scenario: `ProcessDocument` has committed the claim (document
42 = PROCESSING, attempt 1, `locked_at` set), `Submit` returns 202, and then
`MarkSubmitted` fails. Maybe the database connection dropped, or the pod was
killed on exactly that line. Now the database says "PROCESSING, not
submitted", while the Principal has already accepted the document.

### 1. The order of steps is the first line of defence

The order is deliberately **claim (commit) → Submit → save the result**, not
**Submit → insert/update**. In this order, before any request goes out, the
database has already recorded that document 42 is "in flight". Whatever fails
afterwards, the document never disappears from view; at worst it is stuck in
PROCESSING. In the reverse order, a crash after Submit would leave no trace at
all that the document was ever sent.

### 2. The callback usually fixes it on its own

The Principal will still send the callback. `HandleCallback` accepts SUCCESS
from any status other than SUCCESS, and FAILED from PROCESSING, so document
42 still finishes correctly even though `submitted_at` was never set. If the
database is still down when the callback arrives, the callback endpoint
answers 500 and a sensible Principal resends. That is safe because the
handler is idempotent.

### 3. Documents that are really stuck: a lease through `locked_at`

If the callback never arrives (or the worker died **before** Submit), the
document would stay PROCESSING forever. `RecoverStale` (run periodically by
the server, and once at startup) takes documents matching:

```sql
status = 'PROCESSING' AND submitted_at IS NULL AND locked_at < now() - lease
```

and returns them to PENDING (or FAILED if out of attempts). The
`submitted_at` column matters here: without it, recovery could not tell
"worker died" from "Submit succeeded, waiting for the callback", and would
resend thousands of documents that are perfectly fine.

The lease must be longer than the Submit timeout. Otherwise a document whose
request is still in flight could be taken over and sent twice. `NewService`
enforces this rule, and the server refuses to start if
`LEASE_TIMEOUT <= SUBMIT_TIMEOUT`.

### 4. An idempotency key towards the Principal

Recovery means **a document may be sent again**. The only way that does not
become a duplicate at the Principal is to send a key the Principal
recognises. The client in this repository sends
`Idempotency-Key: doc-<id>-attempt-<n>`.

There is a trade-off I should own up to. A per-attempt key protects against
network-level retries (the same request sent twice). But after recovery, the
next claim increments the attempt, so the key changes and the Principal
cannot tell that this is a resend of an earlier attempt that may already have
succeeded. A more correct design for production:

- add a `request_id UUID` column to `documents`;
- generate a new `request_id` only when the document is claimed after a
  **definite** failure (the Principal rejected it, or a FAILED callback);
- recovery from an expired lease does **not** generate a new `request_id`,
  so the resend carries the same key and the Principal returns the same
  result.

This is on the improvement list in the README.

### 5. Reconciliation

If the Principal has a status endpoint (`GET /documents?idempotency_key=...`),
recovery should **ask before resending**:

- The Principal knows the request → mark it submitted / apply its status, no
  resend needed.
- The Principal does not know it → safe to resend.

The same polling is needed for the opposite case: a document has
`submitted_at` but no callback arrives within a reasonable time (say 1 hour).
Callbacks can get lost, for example when the Principal gives up after our
endpoint was down for too long. A reconciliation job asks the Principal for
the status of those documents. This is not implemented because the
Principal's contract in the task only has `Submit`.

### 6. Outbox

The outbox pattern matters when a database change **must** be followed by an
external effect and the two must not come apart. In this design the
`documents` table already acts as the outbox for sending: PENDING means
"needs to be sent", and the workers are the relay. A separate outbox table
would add nothing for Submit.

An outbox becomes necessary when the system has to publish events, for
example "batch INV-001 finished" to Kafka or a webhook to a client. That
event must be written to an `outbox` table in the same transaction as the
last status change, then sent by a separate relay. If it is sent directly
from code after the commit, a crash between the commit and the publish loses
the event.

Conversely, `document_callbacks` with a unique `callback_id` is the **inbox**
pattern: every incoming message is recorded once, and that record is what
provides the dedupe.

### 7. Realistic guarantees

- **Towards the Principal: at-least-once.** Every document is guaranteed to
  be sent at least once (or end up FAILED after 3 attempts), but in crash or
  timeout scenarios it may be sent more than once. Exactly-once delivery over
  a network cannot be guaranteed: the sender cannot tell "request lost" from
  "response lost".
- **Effect at the Principal: exactly-once, only if** the Principal honours
  the idempotency key. That is a contract to agree on with the Principal, not
  something one side can guarantee alone.
- **Callback processing on our side: effectively-once.** Callbacks may arrive
  many times (at-least-once from the Principal), but their effect is applied
  once thanks to the unique constraint and the conditional updates.

---

## D. Analysing a slow query with EXPLAIN ANALYZE

### Before EXPLAIN

First find out which query is slow. Do not guess. `pg_stat_statements` ranks
queries by `total_exec_time` (a 5 ms query called a million times matters
more than a 2-second query called once a day). `auto_explain` with
`log_min_duration` records the real production plan without having to rerun
the query.

### How to read it: a real example from this system

Query #3 (unfinished batches), the first version I wrote:

```sql
WHERE d.batch_id = b.id
  AND (d.status IN ('PENDING','PROCESSING')
       OR (d.status = 'FAILED' AND d.attempt_count < d.max_attempts))
```

```
Limit (actual time=79.790..79.798 rows=10 loops=1)
  Buffers: shared hit=503536
  ->  Nested Loop Semi Join (actual time=79.789..79.798 rows=10 loops=1)
        ->  Index Only Scan using batches_pkey on batches b (actual ... rows=2000 loops=1)
        ->  Index Scan using documents_batch_status_idx on documents d
              (actual time=0.040..0.040 rows=0 loops=2000)
              Index Cond: (batch_id = b.id)
              Filter: ((status = ANY ('{PENDING,PROCESSING}')) OR ((status = 'FAILED') AND (attempt_count < max_attempts)))
              Rows Removed by Filter: 249
              Buffers: shared hit=503510
Execution Time: 79.804 ms
```

How to read it, from the inside out (the most indented node runs first):

- **Index Scan vs Seq Scan.** An index is already in use here, so "uses an
  index" alone does not mean fast. What matters is what is in `Index Cond`
  and what is in `Filter`.
- **`Index Cond` vs `Filter`.** Only `batch_id` is in the Index Cond. The
  `status` condition became a Filter because of the top-level `OR`, so
  PostgreSQL reads **every** row of the batch from the index and discards them
  one by one: `Rows Removed by Filter: 249` per batch.
- **`loops`.** The `rows` and `actual time` figures on the inner node are
  **per loop**. `0.040 ms × loops=2000` ≈ 80 ms; that is where all the time
  goes. A common mistake is to see `0.040 ms` and conclude the node is cheap.
- **Buffers.** `shared hit=503510` = about 500k 8 KB pages (~3.8 GB of
  logical data) read from cache to return 10 rows. `hit` means from shared
  buffers; a `read` would mean from disk/OS cache and be far more expensive.
  Buffers are more stable than timing (timing depends on cache and machine
  load), so I trust this number more for before/after comparisons.

The fix: rewrite the condition so `status` can go into the Index Cond, by
pulling `IN (...)` out as its own AND condition:

```sql
  AND d.status IN ('PENDING','PROCESSING','FAILED')
  AND (d.status <> 'FAILED' OR d.attempt_count < d.max_attempts)
```

```
->  Index Scan using documents_batch_status_idx on documents d (actual time=0.002..0.002 rows=0 loops=2000)
      Index Cond: ((batch_id = b.id) AND (status = ANY ('{PENDING,PROCESSING,FAILED}')))
      Filter: ((status <> 'FAILED') OR (attempt_count < max_attempts))
      Buffers: shared hit=6010
Execution Time: 5.044 ms
```

Same result, 16x faster, 83x fewer buffers, no new index. The next step if
the data grew much larger: this query still loops over every batch. Turning
it around to "find active documents first" (`SELECT DISTINCT batch_id FROM
documents WHERE status IN (...)`) would use a partial index that only holds
active documents.

### Estimated vs actual rows: a sign of stale statistics

After inserting 100k new PENDING documents without `ANALYZE` (autovacuum
temporarily disabled for the demo):

```
Bitmap Heap Scan on documents  (cost=71.00..6239.08 rows=14 width=8)
                               (actual time=1.341..6.165 rows=102000 loops=1)
  ->  Bitmap Index Scan on documents_pending_idx (cost=0.00..71.00 rows=2916 ...)
                                                 (actual ... rows=102000 ...)
```

The planner estimated **14** rows; the reality was **102,000**. A gap of
thousands of times like this is the main sign of stale statistics. In this
simple query the plan happened to stay the same, but in a query with a JOIN
an estimate of 14 rows would make the planner choose a Nested Loop that then
runs 102k times. After `ANALYZE documents`:

```
Bitmap Heap Scan on documents  (cost=2104.28..12858.78 rows=17442 width=8) (actual ... rows=102000 ...)
```

The index scan estimate is now accurate (102,300 vs 102,000), but the Heap
Scan is still off (17,442 vs 102,000). The cause is not stale statistics but
**correlation between columns**: the planner assumes `status = 'PENDING'` and
`attempt_count = 0` are independent, while almost every PENDING document
really is at attempt 0. The fix is extended statistics:

```sql
CREATE STATISTICS documents_status_attempt (dependencies) ON status, attempt_count FROM documents;
ANALYZE documents;
```

A table like `documents` that receives large inserts at once (one batch =
thousands of rows) deserves an `ANALYZE` after bulk inserts, or a smaller
`autovacuum_analyze_scale_factor`, because the default of 10% can be too
infrequent for a table with millions of rows.

### Sorting on disk

```
Sort (actual time=64.928..83.573 rows=100000 loops=1)
  Sort Key: documents.document_no
  Sort Method: external merge  Disk: 3392kB
  Buffers: shared hit=1612, temp read=1257 written=1354
```

`external merge Disk` and `temp read/written` mean the data being sorted did
not fit in `work_mem` (deliberately set to 64 kB here), so PostgreSQL wrote
temporary files. Compare with `Sort Method: quicksort Memory: 3073kB` in the
earlier example, which fit in memory. Ways to fix it:

- do not sort 100k rows at once: use keyset pagination (`q1b`) with `LIMIT`;
- use an index that is already in `ORDER BY` order (`UNIQUE (batch_id,
  document_no)` can serve `ORDER BY document_no` for one batch), or an
  `ORDER BY id` that matches an existing index;
- raise `work_mem` **only for that session/query** (`SET LOCAL work_mem`),
  not globally, because `work_mem` applies per sort node per connection and
  can multiply quickly.

### Other warning signs I look for

- `Seq Scan` with a large `Rows Removed by Filter` on a big table → the filter
  is not indexed, or there is an index that cannot be used (a function on the
  column such as `lower(status)`, an implicit cast, an `OR` like the example
  above).
- High `Heap Fetches` in an `Index Only Scan` → the visibility map is out of
  date, the table needs VACUUM.
- `Bitmap Heap Scan` with `lossy` blocks → the bitmap does not fit in
  `work_mem`.
- A Nested Loop with large `loops` on an unindexed inner side.
- Planning time much larger than execution time → too many partitions or
  indexes, or a very complex query.

### The risk of EXPLAIN ANALYZE on UPDATE/DELETE in production

`EXPLAIN ANALYZE` **really runs the query**. `EXPLAIN ANALYZE UPDATE documents
SET status = 'PENDING' WHERE ...` in production changes data, fires triggers,
and writes WAL. Wrapping it in `BEGIN; ... ROLLBACK;` keeps the changes from
being saved, but still:

- **row locks are held** until ROLLBACK. Meanwhile workers and callbacks that
  touch the same rows are blocked too. For an UPDATE hitting thousands of
  documents, this can look like a small outage;
- dead tuples are still created (bloat) and WAL is still written, which also
  flows to the replicas;
- sequences are not rolled back, and effects outside the database (triggers
  calling `pg_notify`, foreign data wrappers) may still happen.

What I do instead: use `EXPLAIN` without ANALYZE to see the plan; or turn the
UPDATE into a `SELECT` with the same `WHERE` and `EXPLAIN ANALYZE` that
(the expensive part is almost always finding the rows); or run it on a copy
of the database / staging with representative data. If it really has to be
production: inside a transaction that is ROLLED BACK, with short
`SET LOCAL statement_timeout` and `SET LOCAL lock_timeout`, outside peak
hours.
