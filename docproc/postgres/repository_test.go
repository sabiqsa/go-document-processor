package postgres_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"gorm.io/gorm"

	"github.com/sabiqsa/go-document-processor/docproc"
	"github.com/sabiqsa/go-document-processor/docproc/postgres"
	"github.com/sabiqsa/go-document-processor/docproc/repotest"
)

// These integration tests need a real PostgreSQL. Without TEST_DATABASE_URL
// they are skipped, so `go test ./...` still works on a machine with no
// database.
//
//	TEST_DATABASE_URL=postgres://postgres@localhost:5432/docproc_test?sslmode=disable go test ./docproc/postgres/
//
// WARNING: the tests drop and recreate the tables in that database.
func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL integration tests")
	}
	db, err := postgres.Open(dsn, postgres.PoolConfig{MaxOpenConns: 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	})
	resetSchema(t, db)
	return db
}

func resetSchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	schema, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "db", "schema.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("DROP TABLE IF EXISTS document_callbacks, documents, batches CASCADE").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(string(schema)).Error; err != nil {
		t.Fatalf("apply schema.sql: %v", err)
	}
}

func TestRepositoryContract(t *testing.T) {
	db := openTestDB(t)
	repotest.Run(t, func(t *testing.T) docproc.Repository {
		resetSchema(t, db)
		return postgres.New(db)
	})
}

// JSONB rewrites the payload with different key order and whitespace than
// what was sent. A CreateBatch retry with the same content must not be seen
// as a conflict because of that, while genuinely different content must be
// rejected.
func TestCreateBatchConflictOnPostgres(t *testing.T) {
	db := openTestDB(t)
	svc := docproc.NewService(postgres.New(db), principalFunc(func(context.Context, docproc.Document) error { return nil }))
	ctx := context.Background()

	docs := []docproc.Document{{DocumentNo: "A", Payload: []byte(`{"z": 1,   "a": {"y": [1, 2], "x": "IDR"}}`)}}
	first, err := svc.CreateBatch(ctx, "PG-CONFLICT", docs)
	if err != nil {
		t.Fatal(err)
	}

	same := []docproc.Document{{DocumentNo: "A", Payload: []byte(`{"a":{"x":"IDR","y":[1,2]},"z":1}`)}}
	if got, err := svc.CreateBatch(ctx, "PG-CONFLICT", same); err != nil || got.ID != first.ID {
		t.Fatalf("retry with same content: got = %v, err = %v", got, err)
	}

	changed := []docproc.Document{{DocumentNo: "A", Payload: []byte(`{"a":{"x":"IDR","y":[2,1]},"z":1}`)}}
	if _, err := svc.CreateBatch(ctx, "PG-CONFLICT", changed); !errors.Is(err, docproc.ErrBatchConflict) {
		t.Fatalf("different content: error = %v, want ErrBatchConflict", err)
	}
}

// End-to-end Service scenario on PostgreSQL: parallel claims, the concurrency
// limit, and parallel duplicate callbacks, all using the real SQL.
func TestServiceOnPostgres(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.New(db)

	var mu sync.Mutex
	calls := map[string]int{}
	client := principalFunc(func(_ context.Context, d docproc.Document) error {
		mu.Lock()
		calls[d.ID]++
		mu.Unlock()
		return nil
	})
	svc := docproc.NewService(repo, client)
	ctx := context.Background()

	docs := make([]docproc.Document, 30)
	for i := range docs {
		docs[i] = docproc.Document{DocumentNo: "PG-" + string(rune('A'+i)), Payload: []byte(`{"ok":true}`)}
	}
	b, err := svc.CreateBatch(ctx, "PG-BATCH", docs)
	if err != nil {
		t.Fatal(err)
	}

	// Two concurrent ProcessBatch calls + stray ProcessDocument calls for the same document.
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if err := svc.ProcessBatch(ctx, "PG-BATCH"); err != nil {
				t.Error(err)
			}
		})
	}
	for range 5 {
		wg.Go(func() {
			if err := svc.ProcessDocument(ctx, b.Documents[0].ID); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	for _, d := range b.Documents {
		if calls[d.ID] != 1 {
			t.Fatalf("doc %s submitted %d times", d.DocumentNo, calls[d.ID])
		}
	}

	// Parallel duplicate callbacks + late FAILED callbacks.
	for _, d := range b.Documents {
		for range 3 {
			wg.Go(func() {
				cb := docproc.CallbackRequest{CallbackID: "ok-" + d.ID, DocumentID: d.ID, Status: docproc.StatusSuccess}
				if err := svc.HandleCallback(ctx, cb); err != nil {
					t.Error(err)
				}
			})
		}
	}
	wg.Wait()
	for _, d := range b.Documents {
		cb := docproc.CallbackRequest{CallbackID: "late-" + d.ID, DocumentID: d.ID, Status: docproc.StatusFailed}
		if err := svc.HandleCallback(ctx, cb); err != nil {
			t.Fatal(err)
		}
	}

	st, err := svc.GetBatchStatus(ctx, "PG-BATCH")
	if err != nil {
		t.Fatal(err)
	}
	if st.Counts[docproc.StatusSuccess] != 30 || !st.Done || st.State != docproc.BatchCompleted {
		t.Fatalf("status = %+v", st)
	}
	var cbCount int64
	db.Table("document_callbacks").Count(&cbCount)
	if cbCount != 60 { // 30 unique SUCCESS + 30 late FAILED
		t.Fatalf("callback rows = %d, want 60", cbCount)
	}
	if stats := svc.Stats(); stats.CallbacksApplied != 30 || stats.CallbacksDuplicate != 60 || stats.CallbacksIgnored != 30 {
		t.Fatalf("stats = %+v", stats)
	}
}

type principalFunc func(context.Context, docproc.Document) error

func (f principalFunc) Submit(ctx context.Context, d docproc.Document) error { return f(ctx, d) }
