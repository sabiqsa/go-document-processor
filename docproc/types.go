// Package docproc processes batches of documents sent to the Principal API.
//
// The flow is: CreateBatch -> ProcessBatch/ProcessDocument -> (Principal) ->
// HandleCallback -> RetryFailed when needed. GetBatchStatus reports progress.
// Storage sits behind Repository, so the logic here does not know whether the
// data lives in memory or in PostgreSQL.
package docproc

import (
	"context"
	"time"
)

type DocStatus string

const (
	StatusPending    DocStatus = "PENDING"
	StatusProcessing DocStatus = "PROCESSING"
	StatusSuccess    DocStatus = "SUCCESS"
	StatusFailed     DocStatus = "FAILED"
)

// Statuses returns the four statuses in flow order. It is used so the status
// summary always lists every status, including those with a zero count. It
// is a function (a fresh slice each call) rather than a package variable
// that other packages could modify.
func Statuses() []DocStatus {
	return []DocStatus{StatusPending, StatusProcessing, StatusSuccess, StatusFailed}
}

const (
	MaxAttempts   = 3 // 1 initial attempt + 2 retries
	MaxConcurrent = 3 // max concurrent requests to the Principal, global per Service
)

type Document struct {
	ID           string
	BatchID      string
	DocumentNo   string
	Payload      []byte
	Status       DocStatus
	AttemptCount int
	LastError    error
	// NextRetryAt is only set when backoff is enabled. Nil means a FAILED
	// document may be retried immediately.
	NextRetryAt *time.Time
}

type Batch struct {
	ID        string
	BatchNo   string
	Documents []Document
}

// BatchState is the overall progress of a batch.
type BatchState string

const (
	BatchPending    BatchState = "PENDING"     // no document has been touched yet
	BatchInProgress BatchState = "IN_PROGRESS" // something is running or can still be retried
	BatchCompleted  BatchState = "COMPLETED"   // every document is final
)

type BatchStatus struct {
	BatchNo string
	Total   int
	Counts  map[DocStatus]int
	// Retryable is the number of FAILED documents whose attempt_count is
	// still below the maximum.
	Retryable int
	State     BatchState
	// Done is true when there are no PENDING/PROCESSING documents and no
	// FAILED document that may still be retried.
	Done bool
}

type CallbackRequest struct {
	CallbackID string // unique per event from the Principal (idempotency key)
	DocumentID string
	Status     DocStatus // SUCCESS or FAILED
	Message    string
}

// PrincipalClient abstracts the Principal API.
// Submit returns nil when the Principal ACCEPTS the request (the final
// result arrives later through a callback). An error means the request failed.
type PrincipalClient interface {
	Submit(ctx context.Context, doc Document) error
}
