package docproc

import (
	"context"
	"time"
)

// Repository abstracts storage.
//
// Every implementation must guarantee:
//   - All status changes are conditional (compare-and-set). The expected
//     status and the claimed attempt are part of the WHERE condition, so a
//     stale worker can never overwrite another worker's result.
//   - ClaimDocument is atomic: among many concurrent callers, at most one
//     succeeds for a given attempt.
//   - ApplyCallback is atomic: recording the callback and updating the
//     document happen in one unit (mutex or transaction).
//   - Returned values are copies; callers may modify them freely.
type Repository interface {
	// CreateBatch stores the batch and its documents as PENDING. If batchNo
	// already exists it returns the stored batch with created=false.
	CreateBatch(ctx context.Context, batchNo string, docs []Document) (batch *Batch, created bool, err error)

	// GetBatch looks up a batch by batchNo, without its documents.
	GetBatch(ctx context.Context, batchNo string) (*Batch, error)

	// ListDocumentIDs returns the IDs of the batch's documents in the given
	// status, in stable ID order.
	ListDocumentIDs(ctx context.Context, batchID string, status DocStatus) ([]string, error)

	GetDocument(ctx context.Context, documentID string) (Document, error)

	// ClaimDocument: PENDING -> PROCESSING, attempt_count+1, locked_at=now.
	// Succeeds only if the document is PENDING and attempt_count <
	// max_attempts. Otherwise ErrNotClaimable (or ErrDocumentNotFound).
	ClaimDocument(ctx context.Context, documentID string, now time.Time) (Document, error)

	// MarkSubmitted records that the Principal accepted the request for this
	// attempt. The document stays PROCESSING until the callback arrives.
	// Returns ErrStaleAttempt if the document has moved on from this attempt.
	MarkSubmitted(ctx context.Context, documentID string, attempt int, now time.Time) error

	// MarkFailed: PROCESSING -> FAILED for this attempt. Returns
	// ErrStaleAttempt if the document has moved on (for example a SUCCESS
	// callback arrived first).
	MarkFailed(ctx context.Context, documentID string, attempt int, reason string, nextRetryAt *time.Time, now time.Time) error

	// ReleaseClaim returns a document whose processing was cancelled (ctx
	// cancelled) to PENDING. If it is out of attempts it becomes FAILED
	// instead, so it does not sit in a PENDING state that can never be
	// claimed. Returns ErrStaleAttempt if the document has moved on.
	ReleaseClaim(ctx context.Context, documentID string, attempt int, reason string, now time.Time) error

	// ApplyCallback records the callback and then applies the status change.
	// See CallbackOutcome for what the result means.
	ApplyCallback(ctx context.Context, cb CallbackRequest, now time.Time, backoff Backoff) (CallbackOutcome, error)

	// ResetRetryable: FAILED -> PENDING for the batch's documents with
	// attempt_count < max_attempts whose next_retry_at has passed (or is null).
	ResetRetryable(ctx context.Context, batchID string, now time.Time) (int, error)

	CountByStatus(ctx context.Context, batchID string) (StatusSummary, error)

	// RecoverStale takes back PROCESSING documents that were never recorded
	// as submitted and whose locked_at is older than cutoff. These are
	// documents whose worker died midway.
	RecoverStale(ctx context.Context, cutoff time.Time, now time.Time) (int, error)
}

// CallbackOutcome is the result of ApplyCallback.
type CallbackOutcome struct {
	// Duplicate is true if the CallbackID was already recorded. Nothing was
	// changed at all.
	Duplicate bool
	// Applied is true if the document status actually changed. A new
	// callback may still not be applied, for example FAILED arriving after
	// SUCCESS; it is recorded for auditing anyway.
	Applied bool
	Status  DocStatus // document status after the callback was processed
}

// StatusSummary is the per-status aggregate for one batch.
type StatusSummary struct {
	Counts    map[DocStatus]int
	Total     int
	Retryable int // FAILED with attempt_count < max_attempts
	Untouched int // PENDING and never attempted
}

// Backoff returns how long to wait before a document may be retried, based
// on the attempt that just failed. A nil Backoff means no delay.
type Backoff func(attempt int) time.Duration

// NextRetryAt is a helper for Repository implementations: nil means no delay.
func (b Backoff) NextRetryAt(attempt int, now time.Time) *time.Time {
	if b == nil {
		return nil
	}
	d := b(attempt)
	if d <= 0 {
		return nil
	}
	t := now.Add(d)
	return &t
}
