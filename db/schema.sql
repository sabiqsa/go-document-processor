-- PostgreSQL schema for docproc.
--
-- The starting point is the schema given in the task. Every addition is
-- marked "[+]" with the reason. In short:
--   * CHECK constraints so nonsensical data is rejected by the database, not
--     only by the application.
--   * locked_at / submitted_at for leases and recovering stuck documents.
--   * UNIQUE (callback_id) as the callback idempotency key.
--   * Indexes that follow the actual query patterns (see db/queries.sql #8).

CREATE TABLE batches (
    id              BIGSERIAL    PRIMARY KEY,
    batch_no        VARCHAR(64)  NOT NULL UNIQUE,
    -- Kept from the task, but the batch status the application uses is
    -- always derived from the documents table (GetBatchStatus). Updating the
    -- batch row on every document change would make it a row every worker
    -- fights over (hot row). If needed, a periodic job can fill this column
    -- as a cache.
    status          VARCHAR(32)  NOT NULL DEFAULT 'PENDING'
                    -- [+] values limited to match BatchState in Go
                    CHECK (status IN ('PENDING', 'IN_PROGRESS', 'COMPLETED')),
    total_documents INT          NOT NULL DEFAULT 0
                    CHECK (total_documents >= 0),                    -- [+]
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE documents (
    id            BIGSERIAL    PRIMARY KEY,
    -- [+] ON DELETE CASCADE: deleting a batch (e.g. housekeeping) also
    -- removes its documents instead of leaving orphans.
    batch_id      BIGINT       NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    document_no   VARCHAR(64)  NOT NULL,
    payload       JSONB        NOT NULL,
    status        VARCHAR(16)  NOT NULL DEFAULT 'PENDING'
                  CHECK (status IN ('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED')),
    attempt_count INT          NOT NULL DEFAULT 0,
    max_attempts  INT          NOT NULL DEFAULT 3,
    last_error    TEXT,
    next_retry_at TIMESTAMPTZ,
    -- [+] When a worker claimed the document. Used as a lease: a PROCESSING
    -- document whose locked_at is too old and that was never submitted is
    -- considered stuck.
    locked_at     TIMESTAMPTZ,
    -- [+] When the Principal accepted the request (Submit succeeded). Tells
    -- "worker died midway" (NULL) apart from "waiting for the callback"
    -- (set). Without it, recovery could resend a document whose callback is
    -- still on its way.
    submitted_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (batch_id, document_no),

    -- [+] The attempt limit is enforced in the database too. If an
    -- application bug tries a 4th attempt, the UPDATE fails instead of
    -- silently going through.
    CONSTRAINT documents_attempts_chk
        CHECK (max_attempts > 0 AND attempt_count BETWEEN 0 AND max_attempts),
    -- [+] A PROCESSING document must have locked_at, so recovery can always
    -- find it.
    CONSTRAINT documents_processing_locked_chk
        CHECK (status <> 'PROCESSING' OR locked_at IS NOT NULL)
);

CREATE TABLE document_callbacks (
    id          BIGSERIAL    PRIMARY KEY,
    document_id BIGINT       NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    callback_id VARCHAR(128) NOT NULL,
    status      VARCHAR(16)  NOT NULL
                CHECK (status IN ('SUCCESS', 'FAILED')),            -- [+]
    payload     JSONB,
    received_at TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- [+] Idempotency key. CallbackID is unique per event from the Principal,
    -- so it is unique globally (not per document). This constraint is what
    -- guarantees two identical callbacks arriving together are recorded
    -- once: the second transaction waits for the first to commit and then
    -- hits the conflict.
    CONSTRAINT document_callbacks_callback_id_key UNIQUE (callback_id)
);

-- ---------------------------------------------------------------------------
-- Indexes. Full reasoning in db/queries.sql (#8) and docs/answers.md.
-- ---------------------------------------------------------------------------

-- Status summary per batch and documents by batch + status.
-- UNIQUE (batch_id, document_no) above already serves "every document in a
-- batch"; this index is for filtering by batch + status.
CREATE INDEX documents_batch_status_idx ON documents (batch_id, status);

-- Work queue: PENDING documents per batch, ordered by id. This partial index
-- is small because SUCCESS documents (the majority, normally) are not in it.
CREATE INDEX documents_pending_idx ON documents (batch_id, id)
    WHERE status = 'PENDING';

-- Retry candidates: FAILED per batch whose next_retry_at has passed.
CREATE INDEX documents_retry_idx ON documents (batch_id, next_retry_at)
    WHERE status = 'FAILED';

-- Recovering stuck documents (across batches).
CREATE INDEX documents_stale_idx ON documents (locked_at)
    WHERE status = 'PROCESSING' AND submitted_at IS NULL;

-- PostgreSQL does not index foreign key columns automatically. Without this,
-- looking up callbacks per document and deleting documents would seq scan.
CREATE INDEX document_callbacks_document_id_idx ON document_callbacks (document_id);
