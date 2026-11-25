package postgres

import (
	"errors"
	"strconv"
	"time"

	"github.com/sabiqsa/go-document-processor/docproc"
)

// GORM models. They mirror db/schema.sql. The schema is NOT created through
// AutoMigrate, so there is a single source of truth (the reviewed SQL file),
// including CHECK constraints and partial indexes that GORM tags cannot
// express.

type batchRow struct {
	ID             int64 `gorm:"primaryKey"`
	BatchNo        string
	Status         string
	TotalDocuments int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (batchRow) TableName() string { return "batches" }

type documentRow struct {
	ID           int64 `gorm:"primaryKey"`
	BatchID      int64
	DocumentNo   string
	Payload      []byte `gorm:"type:jsonb"`
	Status       string
	AttemptCount int
	MaxAttempts  int
	LastError    *string
	NextRetryAt  *time.Time
	LockedAt     *time.Time
	SubmittedAt  *time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (documentRow) TableName() string { return "documents" }

type callbackRow struct {
	ID         int64 `gorm:"primaryKey"`
	DocumentID int64
	CallbackID string
	Status     string
	Payload    []byte `gorm:"type:jsonb"`
	ReceivedAt time.Time
}

func (callbackRow) TableName() string { return "document_callbacks" }

func (r documentRow) toDomain() docproc.Document {
	d := docproc.Document{
		ID:           formatID(r.ID),
		BatchID:      formatID(r.BatchID),
		DocumentNo:   r.DocumentNo,
		Payload:      r.Payload,
		Status:       docproc.DocStatus(r.Status),
		AttemptCount: r.AttemptCount,
		NextRetryAt:  r.NextRetryAt,
	}
	if r.LastError != nil {
		d.LastError = errors.New(*r.LastError)
	}
	return d
}

func formatID(id int64) string { return strconv.FormatInt(id, 10) }

// parseID: domain IDs are strings. A non-numeric ID cannot exist in the
// table, so it maps to not-found rather than a format error.
func parseID(s string, notFound error) (int64, error) {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id <= 0 {
		return 0, notFound
	}
	return id, nil
}
