// Package postgres implements docproc.Repository on PostgreSQL using GORM
// (pgx driver).
//
// GORM handles plain CRUD. The queries that concurrency correctness depends
// on (the claim with FOR UPDATE SKIP LOCKED, conditional updates with CASE)
// are written as explicit SQL through Raw/Exec, so they match db/queries.sql
// exactly and are easy to review.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/sabiqsa/go-document-processor/docproc"
)

type Repository struct {
	db *gorm.DB
}

var _ docproc.Repository = (*Repository)(nil)

func New(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

const insertChunk = 500

func (r *Repository) CreateBatch(ctx context.Context, batchNo string, docs []docproc.Document) (*docproc.Batch, bool, error) {
	created := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		b := batchRow{BatchNo: batchNo, Status: string(docproc.BatchPending), TotalDocuments: len(docs)}
		// INSERT ... ON CONFLICT (batch_no) DO NOTHING RETURNING id
		// Two concurrent requests with the same batch_no: the second waits for
		// the first to commit, then inserts nothing.
		res := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "batch_no"}}, DoNothing: true}).Create(&b)
		if res.Error != nil {
			return fmt.Errorf("insert batch: %w", res.Error)
		}
		if res.RowsAffected == 0 {
			return nil
		}

		rows := make([]documentRow, len(docs))
		for i, d := range docs {
			rows[i] = documentRow{
				BatchID:     b.ID,
				DocumentNo:  d.DocumentNo,
				Payload:     d.Payload,
				Status:      string(docproc.StatusPending),
				MaxAttempts: docproc.MaxAttempts,
			}
		}
		if err := tx.CreateInBatches(&rows, insertChunk).Error; err != nil {
			return fmt.Errorf("insert documents: %w", err)
		}
		created = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	// Whether just created or already there, return what is stored.
	b, err := r.findBatch(ctx, batchNo)
	if err != nil {
		return nil, false, err
	}
	var rows []documentRow
	if err := r.db.WithContext(ctx).Where("batch_id = ?", b.ID).Order("id").Find(&rows).Error; err != nil {
		return nil, false, fmt.Errorf("load documents: %w", err)
	}
	batch := &docproc.Batch{ID: formatID(b.ID), BatchNo: b.BatchNo, Documents: make([]docproc.Document, len(rows))}
	for i, row := range rows {
		batch.Documents[i] = row.toDomain()
	}
	return batch, created, nil
}

func (r *Repository) GetBatch(ctx context.Context, batchNo string) (*docproc.Batch, error) {
	b, err := r.findBatch(ctx, batchNo)
	if err != nil {
		return nil, err
	}
	return &docproc.Batch{ID: formatID(b.ID), BatchNo: b.BatchNo}, nil
}

func (r *Repository) findBatch(ctx context.Context, batchNo string) (batchRow, error) {
	var b batchRow
	err := r.db.WithContext(ctx).Where("batch_no = ?", batchNo).Take(&b).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return b, docproc.ErrBatchNotFound
	}
	if err != nil {
		return b, fmt.Errorf("get batch: %w", err)
	}
	return b, nil
}

func (r *Repository) ListDocumentIDs(ctx context.Context, batchID string, status docproc.DocStatus) ([]string, error) {
	bid, err := parseID(batchID, docproc.ErrBatchNotFound)
	if err != nil {
		return nil, err
	}
	var ids []int64
	err = r.db.WithContext(ctx).Model(&documentRow{}).
		Where("batch_id = ? AND status = ?", bid, string(status)).
		Order("id").Pluck("id", &ids).Error
	if err != nil {
		return nil, fmt.Errorf("list documents: %w", err)
	}
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = formatID(id)
	}
	return out, nil
}

func (r *Repository) GetDocument(ctx context.Context, documentID string) (docproc.Document, error) {
	id, err := parseID(documentID, docproc.ErrDocumentNotFound)
	if err != nil {
		return docproc.Document{}, err
	}
	var row documentRow
	err = r.db.WithContext(ctx).Take(&row, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return docproc.Document{}, docproc.ErrDocumentNotFound
	}
	if err != nil {
		return docproc.Document{}, fmt.Errorf("get document: %w", err)
	}
	return row.toDomain(), nil
}

// claimSQL: the subquery locks the row with SKIP LOCKED, so if another worker
// is claiming the same document we get 0 rows immediately instead of queueing
// for the lock. status and attempt_count are re-checked under the lock.
const claimSQL = `
UPDATE documents AS d
SET    status        = 'PROCESSING',
       attempt_count = d.attempt_count + 1,
       locked_at     = @now,
       submitted_at  = NULL,
       updated_at    = @now
WHERE  d.id = (
         SELECT id FROM documents
         WHERE  id = @id
           AND  status = 'PENDING'
           AND  attempt_count < max_attempts
         FOR UPDATE SKIP LOCKED
       )
RETURNING d.*`

func (r *Repository) ClaimDocument(ctx context.Context, documentID string, now time.Time) (docproc.Document, error) {
	id, err := parseID(documentID, docproc.ErrDocumentNotFound)
	if err != nil {
		return docproc.Document{}, err
	}
	var rows []documentRow
	if err := r.db.WithContext(ctx).Raw(claimSQL, map[string]any{"id": id, "now": now}).Scan(&rows).Error; err != nil {
		return docproc.Document{}, fmt.Errorf("claim: %w", err)
	}
	if len(rows) == 1 {
		return rows[0].toDomain(), nil
	}
	// 0 rows: tell "does not exist" apart from "cannot be claimed".
	current, err := r.GetDocument(ctx, documentID)
	if err != nil {
		return docproc.Document{}, err
	}
	return docproc.Document{}, fmt.Errorf("%w: status %s, attempt %d", docproc.ErrNotClaimable, current.Status, current.AttemptCount)
}

// updateAttempt runs an UPDATE that only applies while the document is still
// PROCESSING at the same attempt (fencing). 0 rows means ErrStaleAttempt, or
// ErrDocumentNotFound if the document does not exist at all.
func (r *Repository) updateAttempt(ctx context.Context, documentID string, attempt int, values map[string]any) error {
	id, err := parseID(documentID, docproc.ErrDocumentNotFound)
	if err != nil {
		return err
	}
	res := r.db.WithContext(ctx).Model(&documentRow{}).
		Where("id = ? AND status = ? AND attempt_count = ?", id, string(docproc.StatusProcessing), attempt).
		Updates(values)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return r.staleOrMissing(ctx, documentID, attempt)
	}
	return nil
}

func (r *Repository) staleOrMissing(ctx context.Context, documentID string, attempt int) error {
	current, err := r.GetDocument(ctx, documentID)
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: status %s, attempt %d (expected PROCESSING at %d)",
		docproc.ErrStaleAttempt, current.Status, current.AttemptCount, attempt)
}

func (r *Repository) MarkSubmitted(ctx context.Context, documentID string, attempt int, now time.Time) error {
	return r.updateAttempt(ctx, documentID, attempt, map[string]any{
		"submitted_at": now,
		"updated_at":   now,
	})
}

func (r *Repository) MarkFailed(ctx context.Context, documentID string, attempt int, reason string, nextRetryAt *time.Time, now time.Time) error {
	return r.updateAttempt(ctx, documentID, attempt, map[string]any{
		"status":        string(docproc.StatusFailed),
		"last_error":    reason,
		"next_retry_at": nextRetryAt,
		"locked_at":     nil,
		"updated_at":    now,
	})
}

func (r *Repository) ReleaseClaim(ctx context.Context, documentID string, attempt int, reason string, now time.Time) error {
	return r.updateAttempt(ctx, documentID, attempt, map[string]any{
		// Out of attempts: the document cannot go back to PENDING (it could
		// never be claimed again), so it becomes FAILED directly.
		"status":       gorm.Expr("CASE WHEN attempt_count >= max_attempts THEN 'FAILED' ELSE 'PENDING' END"),
		"last_error":   reason,
		"locked_at":    nil,
		"submitted_at": nil,
		"updated_at":   now,
	})
}

// ApplyCallback runs one transaction:
//  1. SELECT ... FOR UPDATE on the document (also checks that it exists),
//  2. INSERT the callback ON CONFLICT (callback_id) DO NOTHING,
//  3. a conditional UPDATE of the document depending on the callback type.
//
// Locking the document first serialises every callback for the same
// document, so the "Applied" result is always accurate.
func (r *Repository) ApplyCallback(ctx context.Context, cb docproc.CallbackRequest, now time.Time, backoff docproc.Backoff) (docproc.CallbackOutcome, error) {
	id, err := parseID(cb.DocumentID, docproc.ErrDocumentNotFound)
	if err != nil {
		return docproc.CallbackOutcome{}, err
	}
	payload, err := json.Marshal(map[string]string{"message": cb.Message})
	if err != nil {
		return docproc.CallbackOutcome{}, err
	}

	var out docproc.CallbackOutcome
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var doc documentRow
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id", "status", "attempt_count").Take(&doc, id).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return docproc.ErrDocumentNotFound
		}
		if err != nil {
			return fmt.Errorf("lock document: %w", err)
		}

		rec := callbackRow{DocumentID: id, CallbackID: cb.CallbackID, Status: string(cb.Status), Payload: payload, ReceivedAt: now}
		res := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "callback_id"}}, DoNothing: true}).Create(&rec)
		if res.Error != nil {
			return fmt.Errorf("insert callback: %w", res.Error)
		}
		if res.RowsAffected == 0 {
			// No row inserted: the callback_id already exists.
			out = docproc.CallbackOutcome{Duplicate: true, Status: docproc.DocStatus(doc.Status)}
			return nil
		}

		upd := tx.Model(&documentRow{}).Where("id = ?", id)
		var values map[string]any
		switch cb.Status {
		case docproc.StatusSuccess:
			upd = upd.Where("status <> ?", string(docproc.StatusSuccess))
			values = map[string]any{
				"status": string(docproc.StatusSuccess), "last_error": nil,
				"next_retry_at": nil, "locked_at": nil, "updated_at": now,
			}
		case docproc.StatusFailed:
			// Stricter than just "<> SUCCESS": FAILED only makes sense for a
			// document that is actually waiting for a result.
			upd = upd.Where("status = ?", string(docproc.StatusProcessing))
			reason := cb.Message
			if reason == "" {
				reason = "principal reported FAILED"
			}
			values = map[string]any{
				"status": string(docproc.StatusFailed), "last_error": reason,
				"next_retry_at": backoff.NextRetryAt(doc.AttemptCount, now),
				"locked_at":     nil, "updated_at": now,
			}
		default:
			return fmt.Errorf("%w: status %q", docproc.ErrInvalidCallback, cb.Status)
		}
		res = upd.Updates(values)
		if res.Error != nil {
			return fmt.Errorf("update document: %w", res.Error)
		}

		out.Status = docproc.DocStatus(doc.Status)
		if res.RowsAffected == 1 {
			out.Applied = true
			out.Status = cb.Status
		}
		return nil
	})
	if err != nil {
		return docproc.CallbackOutcome{}, err
	}
	return out, nil
}

func (r *Repository) ResetRetryable(ctx context.Context, batchID string, now time.Time) (int, error) {
	bid, err := parseID(batchID, docproc.ErrBatchNotFound)
	if err != nil {
		return 0, err
	}
	res := r.db.WithContext(ctx).Model(&documentRow{}).
		Where("batch_id = ? AND status = ? AND attempt_count < max_attempts", bid, string(docproc.StatusFailed)).
		Where("next_retry_at IS NULL OR next_retry_at <= ?", now).
		Updates(map[string]any{
			"status":        string(docproc.StatusPending),
			"next_retry_at": nil,
			"updated_at":    now,
		})
	if res.Error != nil {
		return 0, fmt.Errorf("reset retryable: %w", res.Error)
	}
	return int(res.RowsAffected), nil
}

const countSQL = `
SELECT count(*)                                                        AS total,
       count(*) FILTER (WHERE status = 'PENDING')                      AS pending,
       count(*) FILTER (WHERE status = 'PROCESSING')                   AS processing,
       count(*) FILTER (WHERE status = 'SUCCESS')                      AS success,
       count(*) FILTER (WHERE status = 'FAILED')                       AS failed,
       count(*) FILTER (WHERE status = 'FAILED'
                          AND attempt_count < max_attempts)            AS retryable,
       count(*) FILTER (WHERE status = 'PENDING' AND attempt_count = 0) AS untouched
FROM   documents
WHERE  batch_id = ?`

func (r *Repository) CountByStatus(ctx context.Context, batchID string) (docproc.StatusSummary, error) {
	bid, err := parseID(batchID, docproc.ErrBatchNotFound)
	if err != nil {
		return docproc.StatusSummary{}, err
	}
	var row struct {
		Total, Pending, Processing, Success, Failed, Retryable, Untouched int
	}
	if err := r.db.WithContext(ctx).Raw(countSQL, bid).Scan(&row).Error; err != nil {
		return docproc.StatusSummary{}, fmt.Errorf("count by status: %w", err)
	}
	return docproc.StatusSummary{
		Counts: map[docproc.DocStatus]int{
			docproc.StatusPending:    row.Pending,
			docproc.StatusProcessing: row.Processing,
			docproc.StatusSuccess:    row.Success,
			docproc.StatusFailed:     row.Failed,
		},
		Total:     row.Total,
		Retryable: row.Retryable,
		Untouched: row.Untouched,
	}, nil
}

// recoverSQL is bounded by LIMIT and uses SKIP LOCKED so that several
// instances running recovery at the same time do not wait on each other, and
// a single recovery round does not lock too many rows at once.
const recoverSQL = `
UPDATE documents AS d
SET    status       = CASE WHEN d.attempt_count >= d.max_attempts THEN 'FAILED' ELSE 'PENDING' END,
       last_error   = 'lease expired: worker did not finish',
       locked_at    = NULL,
       updated_at   = @now
WHERE  d.id IN (
         SELECT id FROM documents
         WHERE  status = 'PROCESSING'
           AND  submitted_at IS NULL
           AND  locked_at < @cutoff
         ORDER  BY locked_at
         LIMIT  @limit
         FOR UPDATE SKIP LOCKED
       )`

const recoverLimit = 1000

func (r *Repository) RecoverStale(ctx context.Context, cutoff time.Time, now time.Time) (int, error) {
	res := r.db.WithContext(ctx).Exec(recoverSQL, map[string]any{"now": now, "cutoff": cutoff, "limit": recoverLimit})
	if res.Error != nil {
		return 0, fmt.Errorf("recover stale: %w", res.Error)
	}
	return int(res.RowsAffected), nil
}
