package docproc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"
)

type Service struct {
	repo   Repository
	client PrincipalClient

	// sem limits how many documents are being processed (claim + Submit +
	// persist result) across the whole Service, whether called through
	// ProcessBatch or ProcessDocument directly. One slot = one request to the
	// Principal.
	sem chan struct{}

	now            func() time.Time
	log            *slog.Logger
	backoff        Backoff
	submitTimeout  time.Duration
	leaseTimeout   time.Duration
	persistTimeout time.Duration

	counters counters
}

func NewService(repo Repository, client PrincipalClient, opts ...Option) *Service {
	s := &Service{
		repo:           repo,
		client:         client,
		sem:            make(chan struct{}, MaxConcurrent),
		now:            time.Now,
		log:            slog.New(slog.DiscardHandler),
		submitTimeout:  defaultSubmitTimeout,
		leaseTimeout:   defaultLeaseTimeout,
		persistTimeout: defaultPersistTimeout,
	}
	for _, opt := range opts {
		opt(s)
	}
	// The lease must outlast the Submit timeout. Otherwise RecoverStale could
	// take a document whose request is still in flight and send it twice.
	if s.submitTimeout > 0 && s.leaseTimeout <= s.submitTimeout {
		s.leaseTimeout = 2 * s.submitTimeout
	}
	return s
}

// CreateBatch stores the batch and its documents as PENDING. batchNo is
// unique. Calling again with the same batchNo and the same documents returns
// the existing batch (idempotent, safe for client retries). The same batchNo
// with different documents returns ErrBatchConflict.
func (s *Service) CreateBatch(ctx context.Context, batchNo string, docs []Document) (*Batch, error) {
	if err := validateBatch(batchNo, docs); err != nil {
		return nil, err
	}
	batch, created, err := s.repo.CreateBatch(ctx, batchNo, docs)
	if err != nil {
		return nil, fmt.Errorf("create batch %q: %w", batchNo, err)
	}
	if !created {
		// The same batch_no only counts as a retry if the content matches too.
		// Otherwise the caller most likely reused a batch_no by mistake, and
		// silently returning the old batch would hide that.
		if !sameDocuments(batch.Documents, docs) {
			return nil, fmt.Errorf("create batch %q: %w", batchNo, ErrBatchConflict)
		}
		s.log.InfoContext(ctx, "batch already exists, returning existing", "batch_no", batchNo)
	}
	return batch, nil
}

// sameDocuments compares the stored documents with the request. Document
// order is ignored, and payloads are compared as JSON values rather than
// bytes: PostgreSQL JSONB rewrites the payload with different key order and
// whitespace than what was sent.
func sameDocuments(stored, req []Document) bool {
	if len(stored) != len(req) {
		return false
	}
	payloads := make(map[string][]byte, len(stored))
	for _, d := range stored {
		payloads[d.DocumentNo] = d.Payload
	}
	for _, d := range req {
		p, ok := payloads[d.DocumentNo]
		if !ok || !jsonEqual(p, d.Payload) {
			return false
		}
	}
	return true
}

func jsonEqual(a, b []byte) bool {
	var va, vb any
	if json.Unmarshal(a, &va) != nil || json.Unmarshal(b, &vb) != nil {
		return false
	}
	return reflect.DeepEqual(va, vb)
}

func validateBatch(batchNo string, docs []Document) error {
	if batchNo == "" {
		return fmt.Errorf("%w: batch_no is required", ErrInvalidInput)
	}
	if len(batchNo) > 64 {
		return fmt.Errorf("%w: batch_no longer than 64 chars", ErrInvalidInput)
	}
	if len(docs) == 0 {
		return fmt.Errorf("%w: batch %q has no documents", ErrInvalidInput, batchNo)
	}
	seen := make(map[string]struct{}, len(docs))
	for i, d := range docs {
		switch {
		case d.DocumentNo == "":
			return fmt.Errorf("%w: document #%d has empty document_no", ErrInvalidInput, i)
		case len(d.DocumentNo) > 64:
			return fmt.Errorf("%w: document_no %q longer than 64 chars", ErrInvalidInput, d.DocumentNo)
		case !json.Valid(d.Payload):
			// The payload column is JSONB; better to reject it here than to
			// fail halfway through the transaction.
			return fmt.Errorf("%w: document %q payload is not valid JSON", ErrInvalidInput, d.DocumentNo)
		}
		if _, dup := seen[d.DocumentNo]; dup {
			return fmt.Errorf("%w: duplicate document_no %q", ErrInvalidInput, d.DocumentNo)
		}
		seen[d.DocumentNo] = struct{}{}
	}
	return nil
}

// ProcessBatch processes every PENDING document in the batch concurrently
// (at most MaxConcurrent requests at once, shared with every other caller of
// this Service). It stops cleanly when ctx is cancelled and returns only
// after all workers have finished.
//
// A failed Submit is not a batch-level error (the document is already
// recorded as FAILED and can be retried). It returns ctx.Err() when
// cancelled, or the joined infrastructure errors (e.g. database down).
func (s *Service) ProcessBatch(ctx context.Context, batchNo string) error {
	batch, err := s.repo.GetBatch(ctx, batchNo)
	if err != nil {
		return fmt.Errorf("process batch %q: %w", batchNo, err)
	}
	ids, err := s.repo.ListDocumentIDs(ctx, batch.ID, StatusPending)
	if err != nil {
		return fmt.Errorf("process batch %q: list pending: %w", batchNo, err)
	}
	if len(ids) == 0 {
		return nil
	}

	// The number of goroutines is capped at MaxConcurrent per call, not one
	// per document. The real "3 requests" limit is enforced by s.sem in
	// ProcessDocument, so two parallel ProcessBatch calls still total 3.
	jobs := make(chan string)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for range min(MaxConcurrent, len(ids)) {
		wg.Go(func() {
			for id := range jobs {
				err := s.ProcessDocument(ctx, id)
				if err == nil || errors.Is(err, ErrSubmitFailed) || ctx.Err() != nil {
					continue
				}
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}

feed:
	for _, id := range ids {
		select {
		case jobs <- id:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("process batch %q: %w", batchNo, err)
	}
	return nil
}

// ProcessDocument claims and processes ONE document:
//   - atomic claim PENDING -> PROCESSING (attempt_count incremented)
//   - call PrincipalClient.Submit (without holding any lock)
//   - Submit accepted: stays PROCESSING, waiting for the callback
//   - Submit failed: FAILED + LastError, returns ErrSubmitFailed
//   - ctx cancelled: back to PENDING (FAILED if out of attempts)
//
// Documents that are SUCCESS, FAILED, or held by another worker are skipped
// with a nil return. If a callback settles the document while Submit is
// still running, the callback wins and the Submit result is dropped.
func (s *Service) ProcessDocument(ctx context.Context, documentID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Take a slot first, then claim. In this order, documents still waiting
	// for a slot stay PENDING, so there is nothing to clean up if ctx is
	// cancelled while they wait.
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-s.sem }()

	doc, err := s.repo.ClaimDocument(ctx, documentID, s.now())
	if errors.Is(err, ErrNotClaimable) {
		s.log.DebugContext(ctx, "skip document", "document_id", documentID, "reason", err)
		return nil
	}
	if err != nil {
		return fmt.Errorf("claim document %s: %w", documentID, err)
	}

	submitErr := s.submit(ctx, doc)

	// The Submit result must be persisted even if the caller's ctx is already
	// cancelled; otherwise the document could be stuck in PROCESSING.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.persistTimeout)
	defer cancel()

	switch {
	case submitErr == nil:
		err := s.repo.MarkSubmitted(pctx, doc.ID, doc.AttemptCount, s.now())
		if errors.Is(err, ErrStaleAttempt) {
			// The callback arrived before we got here. Nothing left to record.
			s.log.DebugContext(ctx, "document settled before submit was recorded", "document_id", doc.ID)
			return nil
		}
		if err != nil {
			// The Principal already accepted the request. The document stays
			// PROCESSING and the callback can still complete it, so just
			// report the error.
			return fmt.Errorf("mark document %s submitted: %w", doc.ID, err)
		}
		return nil

	case ctx.Err() != nil:
		// Cancelled from outside (shutdown / cancel), not rejected by the
		// Principal. Put it back in the queue so it can be processed later.
		err := s.repo.ReleaseClaim(pctx, doc.ID, doc.AttemptCount, submitErr.Error(), s.now())
		if err != nil && !errors.Is(err, ErrStaleAttempt) {
			s.log.ErrorContext(ctx, "release claim failed", "document_id", doc.ID, "err", err)
		}
		return ctx.Err()

	default:
		next := s.backoff.NextRetryAt(doc.AttemptCount, s.now())
		err := s.repo.MarkFailed(pctx, doc.ID, doc.AttemptCount, submitErr.Error(), next, s.now())
		if errors.Is(err, ErrStaleAttempt) {
			// A callback settled the document while Submit was failing (e.g.
			// the Principal accepted it but our request timed out). The
			// callback's result stands; this is not a failure.
			s.log.InfoContext(ctx, "submit failed after document was settled",
				"document_id", doc.ID, "attempt", doc.AttemptCount, "err", submitErr)
			return nil
		}
		if err != nil {
			return fmt.Errorf("mark document %s failed: %w", doc.ID, err)
		}
		s.counters.submitFailures.Add(1)
		s.log.WarnContext(ctx, "submit failed", "document_id", doc.ID, "attempt", doc.AttemptCount, "err", submitErr)
		return fmt.Errorf("%w: document %s attempt %d: %w", ErrSubmitFailed, doc.ID, doc.AttemptCount, submitErr)
	}
}

func (s *Service) submit(ctx context.Context, doc Document) (err error) {
	if s.submitTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.submitTimeout)
		defer cancel()
	}
	s.counters.submitCalls.Add(1)
	s.counters.inFlight.Add(1)
	defer s.counters.inFlight.Add(-1)

	// A panic in the client (a bug in a third-party SDK, say) must not crash
	// the process and leave the document in PROCESSING.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("principal client panic: %v", r)
		}
	}()
	return s.client.Submit(ctx, doc)
}

// HandleCallback processes a callback from the Principal idempotently.
//   - A CallbackID that was already processed -> no-op (no error)
//   - SUCCESS may move PROCESSING/FAILED (and PENDING) to SUCCESS
//   - FAILED only applies to PROCESSING documents and never overwrites SUCCESS
func (s *Service) HandleCallback(ctx context.Context, cb CallbackRequest) error {
	if err := validateCallback(cb); err != nil {
		return err
	}
	out, err := s.repo.ApplyCallback(ctx, cb, s.now(), s.backoff)
	if err != nil {
		return fmt.Errorf("handle callback %s: %w", cb.CallbackID, err)
	}

	switch {
	case out.Duplicate:
		s.counters.cbDuplicate.Add(1)
		s.log.DebugContext(ctx, "duplicate callback", "callback_id", cb.CallbackID)
	case out.Applied:
		s.counters.cbApplied.Add(1)
	default:
		s.counters.cbIgnored.Add(1)
		s.log.InfoContext(ctx, "callback recorded but not applied",
			"callback_id", cb.CallbackID, "document_id", cb.DocumentID,
			"callback_status", cb.Status, "current_status", out.Status)
	}
	return nil
}

func validateCallback(cb CallbackRequest) error {
	switch {
	case cb.CallbackID == "":
		return fmt.Errorf("%w: callback_id is required", ErrInvalidCallback)
	case len(cb.CallbackID) > 128:
		return fmt.Errorf("%w: callback_id longer than 128 chars", ErrInvalidCallback)
	case cb.DocumentID == "":
		return fmt.Errorf("%w: document_id is required", ErrInvalidCallback)
	case cb.Status != StatusSuccess && cb.Status != StatusFailed:
		return fmt.Errorf("%w: status must be SUCCESS or FAILED, got %q", ErrInvalidCallback, cb.Status)
	}
	return nil
}

// RetryFailed moves FAILED documents with attempt_count < MaxAttempts (whose
// backoff delay, if any, has passed) back to PENDING and returns how many it
// moved. Documents that are out of attempts stay FAILED.
//
// It deliberately does not reprocess them; the caller calls ProcessBatch
// afterwards. That keeps RetryFailed fast and lets the caller decide when
// the heavy work runs.
func (s *Service) RetryFailed(ctx context.Context, batchNo string) (int, error) {
	batch, err := s.repo.GetBatch(ctx, batchNo)
	if err != nil {
		return 0, fmt.Errorf("retry failed %q: %w", batchNo, err)
	}
	n, err := s.repo.ResetRetryable(ctx, batch.ID, s.now())
	if err != nil {
		return 0, fmt.Errorf("retry failed %q: %w", batchNo, err)
	}
	return n, nil
}

// GetBatchStatus returns the number of documents per status.
func (s *Service) GetBatchStatus(ctx context.Context, batchNo string) (*BatchStatus, error) {
	batch, err := s.repo.GetBatch(ctx, batchNo)
	if err != nil {
		return nil, fmt.Errorf("get batch status %q: %w", batchNo, err)
	}
	sum, err := s.repo.CountByStatus(ctx, batch.ID)
	if err != nil {
		return nil, fmt.Errorf("get batch status %q: %w", batchNo, err)
	}

	counts := make(map[DocStatus]int, 4)
	for _, st := range Statuses() {
		counts[st] = sum.Counts[st]
	}
	done := counts[StatusPending] == 0 && counts[StatusProcessing] == 0 && sum.Retryable == 0

	state := BatchInProgress
	switch {
	case done:
		state = BatchCompleted
	case sum.Untouched == sum.Total:
		state = BatchPending
	}

	return &BatchStatus{
		BatchNo:   batch.BatchNo,
		Total:     sum.Total,
		Counts:    counts,
		Retryable: sum.Retryable,
		State:     state,
		Done:      done,
	}, nil
}

// RecoverStale takes back PROCESSING documents whose worker disappeared (the
// process died after the claim, before the Submit result was saved).
// Documents already recorded as submitted are left alone because they are
// waiting for their callback. The server calls it periodically; it is safe
// to call from many instances.
func (s *Service) RecoverStale(ctx context.Context) (int, error) {
	now := s.now()
	n, err := s.repo.RecoverStale(ctx, now.Add(-s.leaseTimeout), now)
	if err != nil {
		return 0, fmt.Errorf("recover stale documents: %w", err)
	}
	if n > 0 {
		s.counters.recovered.Add(int64(n))
		s.log.WarnContext(ctx, "recovered stale documents", "count", n)
	}
	return n, nil
}
