// Package memory is an in-memory implementation of docproc.Repository.
//
// A single sync.Mutex guards all state. This is deliberate: every operation
// is short (no I/O under the lock), and one lock makes compound operations
// such as "insert callback + update document" atomic for free.
package memory

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/sabiqsa/go-document-processor/docproc"
)

type Repository struct {
	mu sync.Mutex

	seq          int64
	batchesByNo  map[string]*batchRecord
	batchesByID  map[string]*batchRecord
	docs         map[string]*docRecord
	callbacks    map[string]CallbackRecord // key: CallbackID
	cbByDocument map[string][]string
}

type batchRecord struct {
	id, batchNo string
	docIDs      []string // insertion order = ID order
}

type docRecord struct {
	doc         docproc.Document
	maxAttempts int
	lockedAt    *time.Time
	submittedAt *time.Time
	updatedAt   time.Time
}

// CallbackRecord is a received callback. It is exported so tests can check
// that a duplicate really was not recorded twice.
type CallbackRecord struct {
	ID         string
	CallbackID string
	DocumentID string
	Status     docproc.DocStatus
	Message    string
	ReceivedAt time.Time
}

var _ docproc.Repository = (*Repository)(nil)

func New() *Repository {
	return &Repository{
		batchesByNo:  make(map[string]*batchRecord),
		batchesByID:  make(map[string]*batchRecord),
		docs:         make(map[string]*docRecord),
		callbacks:    make(map[string]CallbackRecord),
		cbByDocument: make(map[string][]string),
	}
}

func (r *Repository) nextID() string {
	r.seq++
	return strconv.FormatInt(r.seq, 10)
}

func (r *Repository) CreateBatch(ctx context.Context, batchNo string, docs []docproc.Document) (*docproc.Batch, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if b, ok := r.batchesByNo[batchNo]; ok {
		return r.batchWithDocs(b), false, nil
	}

	b := &batchRecord{id: r.nextID(), batchNo: batchNo, docIDs: make([]string, 0, len(docs))}
	for _, in := range docs {
		id := r.nextID()
		r.docs[id] = &docRecord{
			doc: docproc.Document{
				ID:         id,
				BatchID:    b.id,
				DocumentNo: in.DocumentNo,
				Payload:    slices.Clone(in.Payload),
				Status:     docproc.StatusPending,
			},
			maxAttempts: docproc.MaxAttempts,
		}
		b.docIDs = append(b.docIDs, id)
	}
	r.batchesByNo[batchNo] = b
	r.batchesByID[b.id] = b
	return r.batchWithDocs(b), true, nil
}

func (r *Repository) batchWithDocs(b *batchRecord) *docproc.Batch {
	out := &docproc.Batch{ID: b.id, BatchNo: b.batchNo, Documents: make([]docproc.Document, 0, len(b.docIDs))}
	for _, id := range b.docIDs {
		out.Documents = append(out.Documents, r.docs[id].snapshot())
	}
	return out
}

func (d *docRecord) snapshot() docproc.Document {
	c := d.doc
	c.Payload = slices.Clone(d.doc.Payload)
	if d.doc.NextRetryAt != nil {
		t := *d.doc.NextRetryAt
		c.NextRetryAt = &t
	}
	return c
}

func (r *Repository) GetBatch(ctx context.Context, batchNo string) (*docproc.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.batchesByNo[batchNo]
	if !ok {
		return nil, docproc.ErrBatchNotFound
	}
	return &docproc.Batch{ID: b.id, BatchNo: b.batchNo}, nil
}

func (r *Repository) ListDocumentIDs(ctx context.Context, batchID string, status docproc.DocStatus) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.batchesByID[batchID]
	if !ok {
		return nil, docproc.ErrBatchNotFound
	}
	var ids []string
	for _, id := range b.docIDs {
		if r.docs[id].doc.Status == status {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (r *Repository) GetDocument(ctx context.Context, documentID string) (docproc.Document, error) {
	if err := ctx.Err(); err != nil {
		return docproc.Document{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.docs[documentID]
	if !ok {
		return docproc.Document{}, docproc.ErrDocumentNotFound
	}
	return d.snapshot(), nil
}

func (r *Repository) ClaimDocument(ctx context.Context, documentID string, now time.Time) (docproc.Document, error) {
	if err := ctx.Err(); err != nil {
		return docproc.Document{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.docs[documentID]
	if !ok {
		return docproc.Document{}, docproc.ErrDocumentNotFound
	}
	if d.doc.Status != docproc.StatusPending || d.doc.AttemptCount >= d.maxAttempts {
		return docproc.Document{}, fmt.Errorf("%w: status %s, attempt %d", docproc.ErrNotClaimable, d.doc.Status, d.doc.AttemptCount)
	}
	d.doc.Status = docproc.StatusProcessing
	d.doc.AttemptCount++
	d.lockedAt = &now
	d.submittedAt = nil
	d.updatedAt = now
	return d.snapshot(), nil
}

// processingAt returns the document only if it is still PROCESSING at the
// same attempt. This is a simple fencing token: an old worker cannot change
// a document that has moved on.
func (r *Repository) processingAt(documentID string, attempt int) (*docRecord, error) {
	d, ok := r.docs[documentID]
	if !ok {
		return nil, docproc.ErrDocumentNotFound
	}
	if d.doc.Status != docproc.StatusProcessing || d.doc.AttemptCount != attempt {
		return nil, fmt.Errorf("%w: status %s, attempt %d (expected PROCESSING at %d)",
			docproc.ErrStaleAttempt, d.doc.Status, d.doc.AttemptCount, attempt)
	}
	return d, nil
}

func (r *Repository) MarkSubmitted(ctx context.Context, documentID string, attempt int, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	d, err := r.processingAt(documentID, attempt)
	if d == nil {
		return err
	}
	d.submittedAt = &now
	d.updatedAt = now
	return nil
}

func (r *Repository) MarkFailed(ctx context.Context, documentID string, attempt int, reason string, nextRetryAt *time.Time, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	d, err := r.processingAt(documentID, attempt)
	if d == nil {
		return err
	}
	d.fail(reason, nextRetryAt, now)
	return nil
}

func (d *docRecord) fail(reason string, nextRetryAt *time.Time, now time.Time) {
	d.doc.Status = docproc.StatusFailed
	d.doc.LastError = errors.New(reason)
	d.doc.NextRetryAt = nextRetryAt
	d.lockedAt = nil
	d.updatedAt = now
}

func (r *Repository) ReleaseClaim(ctx context.Context, documentID string, attempt int, reason string, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	d, err := r.processingAt(documentID, attempt)
	if d == nil {
		return err
	}
	r.release(d, reason, now)
	return nil
}

func (r *Repository) release(d *docRecord, reason string, now time.Time) {
	if d.doc.AttemptCount >= d.maxAttempts {
		d.fail(reason, nil, now)
		return
	}
	d.doc.Status = docproc.StatusPending
	d.doc.LastError = errors.New(reason)
	d.lockedAt = nil
	d.submittedAt = nil
	d.updatedAt = now
}

func (r *Repository) ApplyCallback(ctx context.Context, cb docproc.CallbackRequest, now time.Time, backoff docproc.Backoff) (docproc.CallbackOutcome, error) {
	if err := ctx.Err(); err != nil {
		return docproc.CallbackOutcome{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	d, ok := r.docs[cb.DocumentID]
	if !ok {
		return docproc.CallbackOutcome{}, docproc.ErrDocumentNotFound
	}
	if _, seen := r.callbacks[cb.CallbackID]; seen {
		return docproc.CallbackOutcome{Duplicate: true, Status: d.doc.Status}, nil
	}

	r.callbacks[cb.CallbackID] = CallbackRecord{
		ID: r.nextID(), CallbackID: cb.CallbackID, DocumentID: cb.DocumentID,
		Status: cb.Status, Message: cb.Message, ReceivedAt: now,
	}
	r.cbByDocument[cb.DocumentID] = append(r.cbByDocument[cb.DocumentID], cb.CallbackID)

	applied := false
	switch {
	case cb.Status == docproc.StatusSuccess && d.doc.Status != docproc.StatusSuccess:
		d.doc.Status = docproc.StatusSuccess
		d.doc.LastError = nil
		d.doc.NextRetryAt = nil
		d.lockedAt = nil
		d.updatedAt = now
		applied = true
	case cb.Status == docproc.StatusFailed && d.doc.Status == docproc.StatusProcessing:
		reason := cb.Message
		if reason == "" {
			reason = "principal reported FAILED"
		}
		d.fail(reason, backoff.NextRetryAt(d.doc.AttemptCount, now), now)
		applied = true
	}
	return docproc.CallbackOutcome{Applied: applied, Status: d.doc.Status}, nil
}

func (r *Repository) ResetRetryable(ctx context.Context, batchID string, now time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.batchesByID[batchID]
	if !ok {
		return 0, docproc.ErrBatchNotFound
	}
	n := 0
	for _, id := range b.docIDs {
		d := r.docs[id]
		if d.doc.Status != docproc.StatusFailed || d.doc.AttemptCount >= d.maxAttempts {
			continue
		}
		if d.doc.NextRetryAt != nil && d.doc.NextRetryAt.After(now) {
			continue
		}
		d.doc.Status = docproc.StatusPending
		d.doc.NextRetryAt = nil
		d.updatedAt = now
		n++
	}
	return n, nil
}

func (r *Repository) CountByStatus(ctx context.Context, batchID string) (docproc.StatusSummary, error) {
	if err := ctx.Err(); err != nil {
		return docproc.StatusSummary{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.batchesByID[batchID]
	if !ok {
		return docproc.StatusSummary{}, docproc.ErrBatchNotFound
	}
	sum := docproc.StatusSummary{Counts: make(map[docproc.DocStatus]int, 4), Total: len(b.docIDs)}
	for _, id := range b.docIDs {
		d := r.docs[id]
		sum.Counts[d.doc.Status]++
		switch {
		case d.doc.Status == docproc.StatusFailed && d.doc.AttemptCount < d.maxAttempts:
			sum.Retryable++
		case d.doc.Status == docproc.StatusPending && d.doc.AttemptCount == 0:
			sum.Untouched++
		}
	}
	return sum, nil
}

func (r *Repository) RecoverStale(ctx context.Context, cutoff time.Time, now time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, d := range r.docs {
		if d.doc.Status != docproc.StatusProcessing || d.submittedAt != nil || d.lockedAt == nil || !d.lockedAt.Before(cutoff) {
			continue
		}
		r.release(d, "lease expired: worker did not finish", now)
		n++
	}
	return n, nil
}

// Callbacks returns the recorded callbacks for one document (for tests and
// debugging).
func (r *Repository) Callbacks(documentID string) []CallbackRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]CallbackRecord, 0, len(r.cbByDocument[documentID]))
	for _, cbID := range r.cbByDocument[documentID] {
		out = append(out, r.callbacks[cbID])
	}
	return out
}
