package docproc

import "errors"

// Sentinel errors. Callers match them with errors.Is; every layer wraps them
// with %w and adds its own context.
//
// These are package-level variables, but they are never reassigned after
// initialisation, so they do not count as mutable global state.
var (
	ErrBatchNotFound    = errors.New("batch not found")
	ErrDocumentNotFound = errors.New("document not found")
	ErrInvalidInput     = errors.New("invalid input")
	ErrInvalidCallback  = errors.New("invalid callback")

	// ErrBatchConflict is returned by CreateBatch when the batch_no is already
	// taken by a batch with different documents. Calling again with the same
	// documents is still idempotent and does not return this error.
	ErrBatchConflict = errors.New("batch_no already used with different documents")

	// ErrNotClaimable is returned by a Repository when a document cannot be
	// claimed: it is already SUCCESS, held by another worker, FAILED (it has
	// to go through RetryFailed first), or out of attempts.
	ErrNotClaimable = errors.New("document not claimable")

	// ErrStaleAttempt is returned by a Repository when a fenced update
	// (MarkSubmitted, MarkFailed, ReleaseClaim) finds the document is no longer
	// PROCESSING at the given attempt. Typical causes: a callback already
	// settled it, or RecoverStale reclaimed it. Nothing is changed.
	ErrStaleAttempt = errors.New("document is no longer at this attempt")

	// ErrSubmitFailed means the Principal rejected the request or the request
	// failed. The document has already been recorded as FAILED, so this is not
	// an infrastructure error.
	ErrSubmitFailed = errors.New("submit to principal failed")
)
