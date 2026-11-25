-- Supporting queries for docproc (PostgreSQL 13+).
--
-- Every external value is a parameter ($1, $2, ...). In the queries the
-- application uses, the current time is also passed as a parameter (not
-- now()), so it can be tested with a fake clock and every instance uses the
-- same time source as the Go code.
--
-- Each query starts with "-- name: ..." and is checked with PREPARE against
-- db/schema.sql (TestQueriesSQL in docproc/postgres).


-- ===========================================================================
-- 1. All documents for a batch_no, in stable order.
-- ===========================================================================
-- JOIN to batches through batch_no (UNIQUE -> index), then fetch documents
-- through the UNIQUE (batch_id, document_no) index /
-- documents_batch_status_idx.
-- ORDER BY d.id: id is unique and monotonic, so the order is the same on
-- every call (ORDER BY created_at alone can tie).

-- name: q1_documents_by_batch_no
SELECT b.id              AS batch_id,
       b.batch_no,
       b.total_documents,
       b.created_at      AS batch_created_at,
       d.id              AS document_id,
       d.document_no,
       d.status,
       d.attempt_count,
       d.max_attempts,
       d.last_error,
       d.next_retry_at,
       d.updated_at
FROM   batches   b
JOIN   documents d ON d.batch_id = b.id
WHERE  b.batch_no = $1
ORDER  BY d.id;

-- Keyset-paginated version for large batches: pass the last id of the
-- previous page ($2, starting at 0). No OFFSET, because OFFSET still reads
-- and discards every earlier row.
-- name: q1b_documents_by_batch_no_page
SELECT d.id AS document_id, d.document_no, d.status, d.attempt_count, d.updated_at
FROM   batches   b
JOIN   documents d ON d.batch_id = b.id
WHERE  b.batch_no = $1
  AND  d.id > $2
ORDER  BY d.id
LIMIT  $3;


-- ===========================================================================
-- 2. Document count per status for every batch.
-- ===========================================================================
-- One row per batch; all four statuses always appear as columns (0 when
-- absent) thanks to count(*) FILTER. LEFT JOIN so batches without documents
-- still show up with zeros. count(d.id) rather than count(*) so the NULL
-- row produced by the LEFT JOIN is not counted.
-- Keyset-paginated ($1 = last batch id, $2 = page size) because "every
-- batch" can mean hundreds of thousands of rows.

-- name: q2_status_counts_per_batch
SELECT b.id,
       b.batch_no,
       count(d.id)                                         AS total,
       count(d.id) FILTER (WHERE d.status = 'PENDING')     AS pending,
       count(d.id) FILTER (WHERE d.status = 'PROCESSING')  AS processing,
       count(d.id) FILTER (WHERE d.status = 'SUCCESS')     AS success,
       count(d.id) FILTER (WHERE d.status = 'FAILED')      AS failed
FROM   batches b
LEFT   JOIN documents d ON d.batch_id = b.id
WHERE  b.id > $1
GROUP  BY b.id, b.batch_no
ORDER  BY b.id
LIMIT  $2;

-- Single-batch version used by GetBatchStatus. It also counts retryable and
-- untouched documents to derive Done and BatchState.
-- name: q2b_status_counts_one_batch
SELECT count(*)                                                          AS total,
       count(*) FILTER (WHERE status = 'PENDING')                        AS pending,
       count(*) FILTER (WHERE status = 'PROCESSING')                     AS processing,
       count(*) FILTER (WHERE status = 'SUCCESS')                        AS success,
       count(*) FILTER (WHERE status = 'FAILED')                         AS failed,
       count(*) FILTER (WHERE status = 'FAILED'
                          AND attempt_count < max_attempts)              AS retryable,
       count(*) FILTER (WHERE status = 'PENDING' AND attempt_count = 0)  AS untouched
FROM   documents
WHERE  batch_id = $1;


-- ===========================================================================
-- 3. Batches that are not finished.
-- ===========================================================================
-- Not finished = still has PENDING/PROCESSING documents, or FAILED ones
-- that may still be retried. EXISTS stops at the first matching row, so a
-- large batch is not counted in full as it would be with GROUP BY + HAVING.
-- A FAILED document whose next_retry_at is in the future still counts as
-- unfinished, because it will be retried later.
--
-- The shape of the condition is deliberate. The more "natural" version
--   (status IN ('PENDING','PROCESSING') OR (status = 'FAILED' AND attempt_count < max_attempts))
-- leaves status as a Filter instead of an Index Cond, because of the
-- top-level OR. On 500k test documents that took 80 ms / 503k buffers. With
-- IN (...) pulled out as its own AND condition, status becomes an Index Cond
-- on documents_batch_status_idx: 5 ms / 6k buffers. The EXPLAIN output is in
-- section D of docs/answers.md.

-- name: q3_unfinished_batches
SELECT b.id, b.batch_no, b.created_at
FROM   batches b
WHERE  EXISTS (
         SELECT 1
         FROM   documents d
         WHERE  d.batch_id = b.id
           AND  d.status IN ('PENDING', 'PROCESSING', 'FAILED')
           AND  (d.status <> 'FAILED' OR d.attempt_count < d.max_attempts)
       )
ORDER  BY b.id
LIMIT  $1;


-- ===========================================================================
-- 4. FAILED documents that are still eligible for retry.
-- ===========================================================================
-- Eligible: attempt_count < max_attempts AND the backoff delay has passed.
-- The task says "next_retry_at = now()"; it surely means "<=": a document
-- whose retry time passed even a second ago must be included. NULL means no
-- backoff, retry right away.
-- Uses the partial index documents_retry_idx (batch_id, next_retry_at)
-- WHERE status = 'FAILED'.

-- name: q4_retryable_documents
SELECT d.id, d.batch_id, d.document_no, d.attempt_count, d.max_attempts,
       d.last_error, d.next_retry_at
FROM   documents d
WHERE  d.batch_id = $1
  AND  d.status = 'FAILED'
  AND  d.attempt_count < d.max_attempts
  AND  (d.next_retry_at IS NULL OR d.next_retry_at <= $2)
ORDER  BY d.id;

-- The version RetryFailed uses: move them straight back to PENDING.
-- A single UPDATE is already atomic, so there is no need to SELECT first
-- (SELECT then UPDATE opens a race, see section A of docs/answers.md).
-- name: q4b_reset_retryable
UPDATE documents
SET    status        = 'PENDING',
       next_retry_at = NULL,
       updated_at    = $2
WHERE  batch_id = $1
  AND  status = 'FAILED'
  AND  attempt_count < max_attempts
  AND  (next_retry_at IS NULL OR next_retry_at <= $2)
RETURNING id;


-- ===========================================================================
-- 5. Fetch and lock PENDING documents for a worker.
-- ===========================================================================
-- The CTE picks at most $2 documents and locks them with FOR UPDATE SKIP
-- LOCKED: rows locked by another worker are skipped, not waited on. Several
-- workers/instances can take from the queue at once without blocking each
-- other and without getting the same document.
-- The UPDATE in the same statement sets PROCESSING and increments
-- attempt_count. One statement = one implicit transaction; inside an
-- explicit BEGIN ... COMMIT with other queries, the locks are released at
-- COMMIT. attempt_count < max_attempts is re-checked as a safeguard (on top
-- of the CHECK constraint in the schema).

-- name: q5_claim_pending_batch
WITH picked AS (
    SELECT id
    FROM   documents
    WHERE  batch_id = $1
      AND  status = 'PENDING'
      AND  attempt_count < max_attempts
    ORDER  BY id
    LIMIT  $2
    FOR UPDATE SKIP LOCKED
)
UPDATE documents AS d
SET    status        = 'PROCESSING',
       attempt_count = d.attempt_count + 1,
       locked_at     = $3,
       submitted_at  = NULL,
       updated_at    = $3
FROM   picked
WHERE  d.id = picked.id
RETURNING d.id, d.batch_id, d.document_no, d.payload, d.attempt_count;

-- Single-document variant, used by ProcessDocument(documentID). If another
-- transaction holds the row lock, the subquery is empty and the UPDATE hits
-- 0 rows; the application treats that as "another worker has it" and skips.
-- name: q5b_claim_one_document
UPDATE documents AS d
SET    status        = 'PROCESSING',
       attempt_count = d.attempt_count + 1,
       locked_at     = $2,
       submitted_at  = NULL,
       updated_at    = $2
WHERE  d.id = (
         SELECT id FROM documents
         WHERE  id = $1
           AND  status = 'PENDING'
           AND  attempt_count < max_attempts
         FOR UPDATE SKIP LOCKED
       )
RETURNING d.*;

-- After Submit: the result is saved with the fence "status = PROCESSING AND
-- attempt_count = the claimed attempt". A stale worker (say its document was
-- taken over by recovery) cannot overwrite anything; 0 rows comes back to
-- the application as ErrStaleAttempt.
-- name: q5c_mark_submitted
UPDATE documents
SET    submitted_at = $3, updated_at = $3
WHERE  id = $1 AND status = 'PROCESSING' AND attempt_count = $2;

-- name: q5d_mark_failed
UPDATE documents
SET    status = 'FAILED', last_error = $3, next_retry_at = $4,
       locked_at = NULL, updated_at = $5
WHERE  id = $1 AND status = 'PROCESSING' AND attempt_count = $2;


-- ===========================================================================
-- 6. Preventing duplicate callbacks.
-- ===========================================================================
-- The constraint already exists in schema.sql. If the table already holds
-- data in production, build the index CONCURRENTLY first (it does not block
-- writes), then attach the constraint using that index:
--
--   CREATE UNIQUE INDEX CONCURRENTLY document_callbacks_callback_id_key
--       ON document_callbacks (callback_id);
--   ALTER TABLE document_callbacks
--       ADD CONSTRAINT document_callbacks_callback_id_key
--       UNIQUE USING INDEX document_callbacks_callback_id_key;
--
-- Idempotent insert:
-- name: q6_insert_callback_idempotent
INSERT INTO document_callbacks (document_id, callback_id, status, payload, received_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (callback_id) DO NOTHING
RETURNING id;

-- How the application tells new from duplicate:
--   * RETURNING yields 1 row  -> NEW callback, go on to update the document.
--   * RETURNING yields 0 rows -> DUPLICATE, change nothing, answer success
--     to the Principal (no-op).
-- In Go (GORM) this is read from RowsAffected (1 or 0).
-- If two identical callbacks arrive at the same time, the second
-- transaction waits on the unique index until the first one finishes. If
-- the first COMMITs, the second gets a conflict -> 0 rows. If the first
-- ROLLs BACK, the second inserts. So there is no window in which both are
-- treated as new.
-- Not using "ON CONFLICT DO UPDATE ... RETURNING", because it always returns
-- a row, so new and duplicate cannot be told apart, and it causes a needless
-- write.


-- ===========================================================================
-- 7. A FAILED callback never overwrites SUCCESS: the full flow in one transaction.
-- ===========================================================================
-- Order in the application (postgres.Repository.ApplyCallback):
--
--   BEGIN;
--     -- (a) lock the document; this also checks it exists. Every callback
--     --     for the same document is serialised.
--     SELECT id, status, attempt_count FROM documents WHERE id = $1 FOR UPDATE;
--     -- (b) record the callback (query #6). 0 rows -> duplicate -> COMMIT, done.
--     -- (c) conditional update depending on the callback type (below).
--   COMMIT;

-- name: q7a_lock_document
SELECT id, status, attempt_count
FROM   documents
WHERE  id = $1
FOR UPDATE;

-- SUCCESS callback: applies from any status other than SUCCESS (FAILED and
-- PENDING included: if the Principal says it succeeded, there is no point
-- sending it again).
-- name: q7b_apply_success
UPDATE documents
SET    status        = 'SUCCESS',
       last_error    = NULL,
       next_retry_at = NULL,
       locked_at     = NULL,
       updated_at    = $2
WHERE  id = $1
  AND  status <> 'SUCCESS';

-- FAILED callback: only applies to PROCESSING documents. This is stricter
-- than "status <> 'SUCCESS'":
--   * SUCCESS -> unchanged (a late callback, the core of this task)
--   * PENDING -> unchanged (already rescheduled, the callback is stale)
--   * FAILED  -> unchanged (already failed, nothing new to learn)
-- 0 rows updated = callback recorded for auditing but not applied.
-- name: q7c_apply_failed
UPDATE documents
SET    status        = 'FAILED',
       last_error    = $2,
       next_retry_at = $3,
       locked_at     = NULL,
       updated_at    = $4
WHERE  id = $1
  AND  status = 'PROCESSING';

-- Single-statement alternative (no separate SELECT FOR UPDATE), using a
-- data-modifying CTE. For a duplicate callback the ins CTE is empty, so the
-- UPDATE touches nothing. The downside: from the result alone the
-- application cannot tell "duplicate" from "new but not applied", and a
-- missing document only shows up as a foreign key error. That is why the
-- code uses the transaction version above.
-- name: q7d_callback_single_statement
WITH ins AS (
    INSERT INTO document_callbacks (document_id, callback_id, status, payload, received_at)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (callback_id) DO NOTHING
    RETURNING document_id, status
)
UPDATE documents AS d
SET    status     = ins.status,
       last_error = CASE WHEN ins.status = 'FAILED' THEN $6 END,
       locked_at  = NULL,
       updated_at = $5
FROM   ins
WHERE  d.id = ins.document_id
  AND  (   (ins.status = 'SUCCESS' AND d.status <> 'SUCCESS')
        OR (ins.status = 'FAILED'  AND d.status = 'PROCESSING'))
RETURNING d.id, d.status;


-- ===========================================================================
-- 8. Proposed indexes for looking up documents by batch and status.
-- ===========================================================================
-- (Already created in schema.sql; repeated here with the reasoning.)
--
-- a) General composite index:
--      CREATE INDEX documents_batch_status_idx ON documents (batch_id, status);
--    Column order: batch_id first because EVERY query above filters batch_id
--    by equality and it is highly selective (one batch out of millions of
--    documents). status second because its cardinality is low (4 values);
--    in front it would split the index into 4 big groups and be useless for
--    "every document in batch X". In this order the index also serves
--    queries that only filter batch_id (leftmost prefix).
--
-- b) Partial indexes for the work queue:
--      CREATE INDEX documents_pending_idx ON documents (batch_id, id)
--          WHERE status = 'PENDING';
--      CREATE INDEX documents_retry_idx   ON documents (batch_id, next_retry_at)
--          WHERE status = 'FAILED';
--    Normally most documents end up SUCCESS. A partial index only holds the
--    "active" rows, so it is small, fits in memory, and both the claim query
--    (#5) and the retry query (#4) only read this small index.
--    The id column in documents_pending_idx lets ORDER BY id LIMIT n be
--    served straight from the index with no sort.
--    Why two indexes instead of one WHERE status IN ('PENDING','FAILED')?
--    Claim and retry sort by different columns (id vs next_retry_at); two
--    small indexes that fit exactly beat one index that half fits both. The
--    catch: the query predicate must contain exactly the same condition
--    (status = 'PENDING') for the planner to use a partial index.
--
-- c) Cross-batch recovery:
--      CREATE INDEX documents_stale_idx ON documents (locked_at)
--          WHERE status = 'PROCESSING' AND submitted_at IS NULL;
--
-- Trade-offs against write cost:
--   * Every index adds work to every INSERT, and to every UPDATE that
--     touches an indexed column. documents is updated very often (status
--     changes 2-4 times per document), so indexes must not be added lightly.
--   * Updates that change status move rows in and out of partial indexes,
--     and because status is indexed those updates cannot be HOT (heap-only
--     tuple) updates. That is the price paid for fast reads. Columns that
--     change often but are not indexed (last_error, updated_at,
--     submitted_at) can still be HOT if the page has room; a fillfactor
--     below 100 (e.g. 90) helps.
--   * Partial indexes are cheaper to maintain than full ones: SUCCESS rows
--     that never change again do not weigh on the queue indexes.
--   * In production, new indexes are created with CREATE INDEX CONCURRENTLY
--     so writes are not blocked while they build.
