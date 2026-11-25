package docproc_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/sabiqsa/go-document-processor/docproc"
)

func TestCreateBatch_Validation(t *testing.T) {
	doc := func(no, payload string) docproc.Document {
		return docproc.Document{DocumentNo: no, Payload: []byte(payload)}
	}
	tests := []struct {
		name    string
		batchNo string
		docs    []docproc.Document
	}{
		{"empty batch_no", "", []docproc.Document{doc("A", `{}`)}},
		{"no documents", "B", nil},
		{"empty document_no", "B", []docproc.Document{doc("", `{}`)}},
		{"duplicate document_no", "B", []docproc.Document{doc("A", `{}`), doc("A", `{}`)}},
		{"payload is not JSON", "B", []docproc.Document{doc("A", `not-json`)}},
		{"empty payload", "B", []docproc.Document{doc("A", ``)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(nil)
			_, err := f.svc.CreateBatch(context.Background(), tt.batchNo, tt.docs)
			if !errors.Is(err, docproc.ErrInvalidInput) {
				t.Fatalf("error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestCreateBatch_Idempotent(t *testing.T) {
	f := newFixture(nil)
	ctx := context.Background()

	first, err := f.svc.CreateBatch(ctx, "INV-001", makeDocs(3))
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range first.Documents {
		if d.Status != docproc.StatusPending || d.ID == "" || d.BatchID != first.ID {
			t.Fatalf("invalid new document: %+v", d)
		}
	}

	// Call again (e.g. a client retrying after a timeout), even in parallel.
	var wg sync.WaitGroup
	ids := make([]string, 10)
	for i := range ids {
		wg.Go(func() {
			b, err := f.svc.CreateBatch(ctx, "INV-001", makeDocs(3))
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = b.ID
		})
	}
	wg.Wait()
	for _, id := range ids {
		if id != first.ID {
			t.Fatalf("duplicate batch created: %s vs %s", id, first.ID)
		}
	}
	if st := f.status(t, "INV-001"); st.Total != 3 {
		t.Fatalf("total documents = %d, want 3", st.Total)
	}
}

// The same batch_no with different documents must be rejected instead of
// silently returning the old batch. The same documents are still idempotent
// even if the document order or the JSON formatting differs.
func TestCreateBatch_Conflict(t *testing.T) {
	doc := func(no, payload string) docproc.Document {
		return docproc.Document{DocumentNo: no, Payload: []byte(payload)}
	}
	original := []docproc.Document{doc("A", `{"amount":1000,"currency":"IDR"}`), doc("B", `{"amount":2500}`)}

	tests := []struct {
		name     string
		docs     []docproc.Document
		conflict bool
	}{
		{"identical", original, false},
		{"different document order", []docproc.Document{original[1], original[0]}, false},
		{"different JSON key order and spacing", []docproc.Document{doc("A", `{ "currency": "IDR", "amount": 1000 }`), original[1]}, false},
		{"different payload", []docproc.Document{doc("A", `{"amount":9999,"currency":"IDR"}`), original[1]}, true},
		{"fewer documents", original[:1], true},
		{"more documents", append(slices.Clone(original), doc("C", `{}`)), true},
		{"different document_no", []docproc.Document{doc("X", string(original[0].Payload)), original[1]}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(nil)
			ctx := context.Background()
			first, err := f.svc.CreateBatch(ctx, "INV-C", original)
			if err != nil {
				t.Fatal(err)
			}

			got, err := f.svc.CreateBatch(ctx, "INV-C", tt.docs)
			if tt.conflict {
				if !errors.Is(err, docproc.ErrBatchConflict) {
					t.Fatalf("error = %v, want ErrBatchConflict", err)
				}
			} else if err != nil || got.ID != first.ID {
				t.Fatalf("got = %v, err = %v, want batch %s with no error", got, err, first.ID)
			}

			// Whatever the outcome, the stored batch is unchanged.
			if st := f.status(t, "INV-C"); st.Total != len(original) {
				t.Fatalf("total documents = %d, want %d", st.Total, len(original))
			}
		})
	}
}

func TestNotFoundErrors(t *testing.T) {
	f := newFixture(nil)
	ctx := context.Background()

	if err := f.svc.ProcessBatch(ctx, "nope"); !errors.Is(err, docproc.ErrBatchNotFound) {
		t.Errorf("ProcessBatch: %v", err)
	}
	if _, err := f.svc.RetryFailed(ctx, "nope"); !errors.Is(err, docproc.ErrBatchNotFound) {
		t.Errorf("RetryFailed: %v", err)
	}
	if _, err := f.svc.GetBatchStatus(ctx, "nope"); !errors.Is(err, docproc.ErrBatchNotFound) {
		t.Errorf("GetBatchStatus: %v", err)
	}
	if err := f.svc.ProcessDocument(ctx, "404"); !errors.Is(err, docproc.ErrDocumentNotFound) {
		t.Errorf("ProcessDocument: %v", err)
	}
}

func TestGetBatchStatus(t *testing.T) {
	f := newFixture(nil)
	b := f.createBatch(t, "BATCH-ST", 4)
	ctx := context.Background()
	ids := []string{b.Documents[0].ID, b.Documents[1].ID, b.Documents[2].ID}

	st := f.status(t, "BATCH-ST")
	if st.State != docproc.BatchPending || st.Done {
		t.Fatalf("new batch: %+v", st)
	}
	// Every status must be in the map, including zero counts.
	for _, s := range docproc.Statuses() {
		if _, ok := st.Counts[s]; !ok {
			t.Errorf("status %s missing from Counts", s)
		}
	}

	_ = f.svc.ProcessDocument(ctx, ids[0])
	_ = f.svc.ProcessDocument(ctx, ids[1])
	f.client.failFn = errFail
	_ = f.svc.ProcessDocument(ctx, ids[2])
	_ = f.svc.HandleCallback(ctx, docproc.CallbackRequest{CallbackID: "c0", DocumentID: ids[0], Status: docproc.StatusSuccess})

	st = f.status(t, "BATCH-ST")
	want := map[docproc.DocStatus]int{
		docproc.StatusPending: 1, docproc.StatusProcessing: 1,
		docproc.StatusSuccess: 1, docproc.StatusFailed: 1,
	}
	for s, n := range want {
		if st.Counts[s] != n {
			t.Errorf("Counts[%s] = %d, want %d", s, st.Counts[s], n)
		}
	}
	if st.State != docproc.BatchInProgress || st.Retryable != 1 || st.Total != 4 {
		t.Errorf("status = %+v", st)
	}
}

// A panic in the client must not crash the process or leave the document in
// PROCESSING.
func TestProcessDocument_ClientPanic(t *testing.T) {
	client := newFakePrincipal()
	client.failFn = func(docproc.Document) error { panic("sdk bug") }
	f := newFixture(client)
	id := f.createBatch(t, "B", 1).Documents[0].ID

	err := f.svc.ProcessDocument(context.Background(), id)
	if !errors.Is(err, docproc.ErrSubmitFailed) {
		t.Fatalf("error = %v, want ErrSubmitFailed", err)
	}
	if got := f.doc(t, id).Status; got != docproc.StatusFailed {
		t.Fatalf("status = %s, want FAILED", got)
	}
}
