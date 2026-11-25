package docproc_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/sabiqsa/go-document-processor/docproc"
)

// Test point 4: a callback with the same CallbackID sent many times, both
// sequentially and in parallel. The final status is right, there is a single
// callback record, and the effect ("applied" counter) happens once.
func TestHandleCallback_Duplicate(t *testing.T) {
	tests := []struct {
		name     string
		parallel bool
		status   docproc.DocStatus
	}{
		{"sequential SUCCESS", false, docproc.StatusSuccess},
		{"parallel SUCCESS", true, docproc.StatusSuccess},
		{"sequential FAILED", false, docproc.StatusFailed},
		{"parallel FAILED", true, docproc.StatusFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(nil)
			b := f.createBatch(t, "BATCH-CB", 1)
			id := b.Documents[0].ID
			ctx := context.Background()
			if err := f.svc.ProcessDocument(ctx, id); err != nil {
				t.Fatal(err)
			}

			cb := docproc.CallbackRequest{CallbackID: "evt-1", DocumentID: id, Status: tt.status, Message: "from principal"}
			const n = 50
			send := func() {
				if err := f.svc.HandleCallback(ctx, cb); err != nil {
					t.Errorf("HandleCallback: %v", err)
				}
			}
			if tt.parallel {
				start := make(chan struct{})
				var wg sync.WaitGroup
				for range n {
					wg.Go(func() { <-start; send() })
				}
				close(start)
				wg.Wait()
			} else {
				for range n {
					send()
				}
			}

			if got := f.doc(t, id).Status; got != tt.status {
				t.Errorf("status = %s, want %s", got, tt.status)
			}
			if recs := f.repo.Callbacks(id); len(recs) != 1 {
				t.Errorf("callback records = %d, want 1", len(recs))
			}
			st := f.svc.Stats()
			if st.CallbacksApplied != 1 || st.CallbacksDuplicate != n-1 {
				t.Errorf("applied=%d duplicate=%d, want 1 and %d", st.CallbacksApplied, st.CallbacksDuplicate, n-1)
			}
		})
	}
}

// Test point 5: SUCCESS then FAILED (different CallbackIDs). Status stays SUCCESS.
func TestHandleCallback_LateFailedKeepsSuccess(t *testing.T) {
	f := newFixture(nil)
	b := f.createBatch(t, "BATCH-LATE", 1)
	id := b.Documents[0].ID
	ctx := context.Background()
	if err := f.svc.ProcessDocument(ctx, id); err != nil {
		t.Fatal(err)
	}

	must := func(cb docproc.CallbackRequest) {
		t.Helper()
		if err := f.svc.HandleCallback(ctx, cb); err != nil {
			t.Fatal(err)
		}
	}
	must(docproc.CallbackRequest{CallbackID: "evt-ok", DocumentID: id, Status: docproc.StatusSuccess})
	must(docproc.CallbackRequest{CallbackID: "evt-late", DocumentID: id, Status: docproc.StatusFailed, Message: "late"})

	d := f.doc(t, id)
	if d.Status != docproc.StatusSuccess || d.LastError != nil {
		t.Fatalf("doc = %s (err %v), want SUCCESS with no error", d.Status, d.LastError)
	}
	// The late callback is still recorded for auditing, just not applied.
	if recs := f.repo.Callbacks(id); len(recs) != 2 {
		t.Errorf("callback records = %d, want 2", len(recs))
	}
	if st := f.svc.Stats(); st.CallbacksIgnored != 1 {
		t.Errorf("CallbacksIgnored = %d, want 1", st.CallbacksIgnored)
	}
}

// Same as above, but SUCCESS and FAILED arrive at the same time. Whatever
// the order, the end result must be SUCCESS.
func TestHandleCallback_SuccessAndFailedRace(t *testing.T) {
	for range 20 {
		f := newFixture(nil)
		b := f.createBatch(t, "B", 1)
		id := b.Documents[0].ID
		ctx := context.Background()
		_ = f.svc.ProcessDocument(ctx, id)

		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, st := range []docproc.DocStatus{docproc.StatusSuccess, docproc.StatusFailed} {
			wg.Go(func() {
				<-start
				cb := docproc.CallbackRequest{CallbackID: string(rune('a' + i)), DocumentID: id, Status: st}
				if err := f.svc.HandleCallback(ctx, cb); err != nil {
					t.Error(err)
				}
			})
		}
		close(start)
		wg.Wait()
		if got := f.doc(t, id).Status; got != docproc.StatusSuccess {
			t.Fatalf("status = %s, want SUCCESS", got)
		}
	}
}

// Transition table: document status when the callback arrives -> status after.
func TestHandleCallback_Transitions(t *testing.T) {
	type setupFn func(t *testing.T, f fixture, id string)
	ctx := context.Background()

	pending := func(*testing.T, fixture, string) {}
	processing := func(t *testing.T, f fixture, id string) {
		if err := f.svc.ProcessDocument(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	failed := func(t *testing.T, f fixture, id string) {
		f.client.failFn = errFail
		_ = f.svc.ProcessDocument(ctx, id)
		f.client.failFn = nil
	}
	success := func(t *testing.T, f fixture, id string) {
		processing(t, f, id)
		cb := docproc.CallbackRequest{CallbackID: "setup", DocumentID: id, Status: docproc.StatusSuccess}
		if err := f.svc.HandleCallback(ctx, cb); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name  string
		setup setupFn
		cb    docproc.DocStatus
		want  docproc.DocStatus
	}{
		{"PROCESSING + SUCCESS", processing, docproc.StatusSuccess, docproc.StatusSuccess},
		{"PROCESSING + FAILED", processing, docproc.StatusFailed, docproc.StatusFailed},
		{"FAILED + SUCCESS", failed, docproc.StatusSuccess, docproc.StatusSuccess},
		{"FAILED + FAILED", failed, docproc.StatusFailed, docproc.StatusFailed},
		{"SUCCESS + FAILED (late)", success, docproc.StatusFailed, docproc.StatusSuccess},
		{"SUCCESS + SUCCESS", success, docproc.StatusSuccess, docproc.StatusSuccess},
		// A SUCCESS callback for an old attempt that arrives after the
		// document was reset to PENDING is still accepted: the Principal
		// already succeeded, so there is no point sending it again.
		{"PENDING + SUCCESS", pending, docproc.StatusSuccess, docproc.StatusSuccess},
		{"PENDING + FAILED (stale)", pending, docproc.StatusFailed, docproc.StatusPending},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(nil)
			id := f.createBatch(t, "B", 1).Documents[0].ID
			tt.setup(t, f, id)

			cb := docproc.CallbackRequest{CallbackID: "evt", DocumentID: id, Status: tt.cb}
			if err := f.svc.HandleCallback(ctx, cb); err != nil {
				t.Fatal(err)
			}
			if got := f.doc(t, id).Status; got != tt.want {
				t.Errorf("status = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestHandleCallback_Validation(t *testing.T) {
	f := newFixture(nil)
	id := f.createBatch(t, "B", 1).Documents[0].ID
	ctx := context.Background()

	tests := []struct {
		name string
		cb   docproc.CallbackRequest
		want error
	}{
		{"no callback id", docproc.CallbackRequest{DocumentID: id, Status: docproc.StatusSuccess}, docproc.ErrInvalidCallback},
		{"no document id", docproc.CallbackRequest{CallbackID: "x", Status: docproc.StatusSuccess}, docproc.ErrInvalidCallback},
		{"status PROCESSING", docproc.CallbackRequest{CallbackID: "x", DocumentID: id, Status: docproc.StatusProcessing}, docproc.ErrInvalidCallback},
		{"unknown status", docproc.CallbackRequest{CallbackID: "x", DocumentID: id, Status: "DONE"}, docproc.ErrInvalidCallback},
		{"document does not exist", docproc.CallbackRequest{CallbackID: "x", DocumentID: "999", Status: docproc.StatusSuccess}, docproc.ErrDocumentNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := f.svc.HandleCallback(ctx, tt.cb); !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}
