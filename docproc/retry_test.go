package docproc_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sabiqsa/go-document-processor/docproc"
)

// Test point 2: the Principal keeps failing. After ProcessBatch + RetryFailed
// are repeated many times, there are exactly 3 Submits per document and a
// 4th attempt never happens.
func TestRetry_StopsAtMaxAttempts(t *testing.T) {
	client := newFakePrincipal()
	client.failFn = errFail
	f := newFixture(client)
	b := f.createBatch(t, "BATCH-RETRY", 5)
	ctx := context.Background()

	if err := f.svc.ProcessBatch(ctx, "BATCH-RETRY"); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	// Deliberately more rounds than the retry budget.
	for round := range 5 {
		n, err := f.svc.RetryFailed(ctx, "BATCH-RETRY")
		if err != nil {
			t.Fatalf("RetryFailed round %d: %v", round, err)
		}
		wantRetried := 0
		if round < docproc.MaxAttempts-1 {
			wantRetried = 5
		}
		if n != wantRetried {
			t.Fatalf("round %d: RetryFailed = %d, want %d", round, n, wantRetried)
		}
		if err := f.svc.ProcessBatch(ctx, "BATCH-RETRY"); err != nil {
			t.Fatalf("ProcessBatch round %d: %v", round, err)
		}
	}

	for _, d := range b.Documents {
		if c := client.callsFor(d.ID); c != docproc.MaxAttempts {
			t.Errorf("doc %s: %d Submits, want %d", d.DocumentNo, c, docproc.MaxAttempts)
		}
		got := f.doc(t, d.ID)
		if got.Status != docproc.StatusFailed || got.AttemptCount != docproc.MaxAttempts {
			t.Errorf("doc %s = %s attempt %d, want FAILED attempt 3", d.DocumentNo, got.Status, got.AttemptCount)
		}
		if got.LastError == nil {
			t.Errorf("doc %s: LastError is empty", d.DocumentNo)
		}
	}
	st := f.status(t, "BATCH-RETRY")
	if !st.Done || st.State != docproc.BatchCompleted || st.Retryable != 0 {
		t.Errorf("status = %+v, want Done/COMPLETED with nothing retryable", st)
	}
}

// Variation: fail N times, then get accepted.
func TestRetry_RecoversBeforeLimit(t *testing.T) {
	tests := []struct {
		name       string
		failTimes  int
		wantCalls  int
		wantStatus docproc.DocStatus
	}{
		{"accepted right away", 0, 1, docproc.StatusProcessing},
		{"fails once", 1, 2, docproc.StatusProcessing},
		{"fails twice, last attempt accepted", 2, 3, docproc.StatusProcessing},
		{"always fails", 99, 3, docproc.StatusFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newFakePrincipal()
			client.failFn = func(d docproc.Document) error {
				if d.AttemptCount <= tt.failTimes {
					return errFail(d)
				}
				return nil
			}
			f := newFixture(client)
			b := f.createBatch(t, "B", 1)
			ctx := context.Background()

			_ = f.svc.ProcessBatch(ctx, "B")
			for range docproc.MaxAttempts + 1 {
				if _, err := f.svc.RetryFailed(ctx, "B"); err != nil {
					t.Fatal(err)
				}
				_ = f.svc.ProcessBatch(ctx, "B")
			}

			id := b.Documents[0].ID
			if c := client.callsFor(id); c != tt.wantCalls {
				t.Errorf("%d Submits, want %d", c, tt.wantCalls)
			}
			if got := f.doc(t, id).Status; got != tt.wantStatus {
				t.Errorf("status %s, want %s", got, tt.wantStatus)
			}
		})
	}
}

// Test point 8: ProcessBatch twice; SUCCESS documents are not sent again.
func TestProcessBatch_SkipsSuccess(t *testing.T) {
	f := newFixture(nil)
	b := f.createBatch(t, "BATCH-OK", 6)
	ctx := context.Background()

	if err := f.svc.ProcessBatch(ctx, "BATCH-OK"); err != nil {
		t.Fatal(err)
	}
	for _, d := range b.Documents {
		cb := docproc.CallbackRequest{CallbackID: "ok-" + d.ID, DocumentID: d.ID, Status: docproc.StatusSuccess}
		if err := f.svc.HandleCallback(ctx, cb); err != nil {
			t.Fatal(err)
		}
	}
	before := f.client.totalCalls()

	if err := f.svc.ProcessBatch(ctx, "BATCH-OK"); err != nil {
		t.Fatal(err)
	}
	for _, d := range b.Documents {
		if err := f.svc.ProcessDocument(ctx, d.ID); err != nil {
			t.Fatalf("ProcessDocument on SUCCESS must be skipped without error, got %v", err)
		}
	}
	if n, _ := f.svc.RetryFailed(ctx, "BATCH-OK"); n != 0 {
		t.Errorf("RetryFailed = %d, want 0", n)
	}

	if after := f.client.totalCalls(); after != before {
		t.Fatalf("Submits went from %d to %d for SUCCESS documents", before, after)
	}
	if st := f.status(t, "BATCH-OK"); st.Counts[docproc.StatusSuccess] != 6 || !st.Done {
		t.Errorf("status = %+v", st)
	}
}

// A Submit that never finishes must be cut off by the submit timeout and the
// document recorded as FAILED, not left stuck in PROCESSING.
// Inside synctest time is virtual: the 30-second timeout fires instantly and
// always exactly.
func TestProcessDocument_SubmitTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newFakePrincipal()
		client.gate = make(chan struct{}) // never opened
		f := newFixture(client, docproc.WithSubmitTimeout(30*time.Second))
		b := f.createBatch(t, "BATCH-TO", 1)
		id := b.Documents[0].ID

		err := f.svc.ProcessDocument(context.Background(), id)
		if !errors.Is(err, docproc.ErrSubmitFailed) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want ErrSubmitFailed + DeadlineExceeded", err)
		}
		d := f.doc(t, id)
		if d.Status != docproc.StatusFailed || d.AttemptCount != 1 {
			t.Fatalf("doc = %s attempt %d, want FAILED attempt 1", d.Status, d.AttemptCount)
		}
		if n, _ := f.svc.RetryFailed(context.Background(), "BATCH-TO"); n != 1 {
			t.Fatalf("a timed-out document must be retryable, RetryFailed = %d", n)
		}
	})
}

// The Principal can accept a document and send its callback while our side
// still sees Submit as failed (for example our request timed out after the
// Principal had already accepted it). The callback's result must stand, and
// the late failure must not be reported or counted as one.
func TestProcessDocument_CallbackSettlesDuringSubmit(t *testing.T) {
	tests := []struct {
		name      string
		submitErr bool
	}{
		{"submit then fails", true},
		{"submit then succeeds", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newFakePrincipal()
			f := newFixture(client)
			ctx := context.Background()
			client.failFn = func(d docproc.Document) error {
				cb := docproc.CallbackRequest{CallbackID: "early-" + d.ID, DocumentID: d.ID, Status: docproc.StatusSuccess}
				if err := f.svc.HandleCallback(ctx, cb); err != nil {
					t.Errorf("HandleCallback: %v", err)
				}
				if tt.submitErr {
					return errFail(d)
				}
				return nil
			}
			id := f.createBatch(t, "BATCH-EARLY", 1).Documents[0].ID

			if err := f.svc.ProcessDocument(ctx, id); err != nil {
				t.Fatalf("ProcessDocument = %v, want nil (the callback already settled it)", err)
			}
			if got := f.doc(t, id).Status; got != docproc.StatusSuccess {
				t.Fatalf("status = %s, want SUCCESS", got)
			}
			if n := f.svc.Stats().SubmitFailures; n != 0 {
				t.Errorf("SubmitFailures = %d, want 0", n)
			}
		})
	}
}

func TestBackoff_DelaysRetry(t *testing.T) {
	clock := newManualClock()
	client := newFakePrincipal()
	client.failFn = errFail
	f := newFixture(client,
		docproc.WithClock(clock.Now),
		docproc.WithBackoff(func(attempt int) time.Duration { return time.Duration(attempt) * time.Minute }),
	)
	b := f.createBatch(t, "BATCH-BO", 1)
	ctx := context.Background()

	_ = f.svc.ProcessBatch(ctx, "BATCH-BO")
	d := f.doc(t, b.Documents[0].ID)
	if d.NextRetryAt == nil || !d.NextRetryAt.Equal(clock.Now().Add(time.Minute)) {
		t.Fatalf("NextRetryAt = %v, want now+1m", d.NextRetryAt)
	}

	if n, _ := f.svc.RetryFailed(ctx, "BATCH-BO"); n != 0 {
		t.Fatalf("retry before the delay = %d, want 0", n)
	}
	// While the delay is pending, the batch is not considered done.
	if st := f.status(t, "BATCH-BO"); st.Done || st.Retryable != 1 {
		t.Fatalf("status = %+v, want not Done and 1 retryable", st)
	}

	clock.Advance(time.Minute)
	if n, _ := f.svc.RetryFailed(ctx, "BATCH-BO"); n != 1 {
		t.Fatalf("retry after the delay = %d, want 1", n)
	}
}

func TestExponentialBackoff(t *testing.T) {
	b := docproc.ExponentialBackoff(time.Second, 10*time.Second)
	cases := []struct {
		attempt  int
		min, max time.Duration
	}{
		{1, 500 * time.Millisecond, time.Second},
		{2, time.Second, 2 * time.Second},
		{3, 2 * time.Second, 4 * time.Second},
		{10, 5 * time.Second, 10 * time.Second}, // hits the max
		{80, 5 * time.Second, 10 * time.Second}, // shift overflow is still safe
	}
	for _, c := range cases {
		for range 200 {
			if d := b(c.attempt); d < c.min || d > c.max {
				t.Fatalf("attempt %d: %v outside [%v, %v]", c.attempt, d, c.min, c.max)
			}
		}
	}
}

// A worker dies after the claim (simulated by claiming directly in the
// repository without a Submit). Once the lease expires, RecoverStale puts it
// back to PENDING. A document already recorded as submitted must not be
// taken, because it is waiting for its callback.
func TestRecoverStale(t *testing.T) {
	clock := newManualClock()
	f := newFixture(nil,
		docproc.WithClock(clock.Now),
		docproc.WithSubmitTimeout(10*time.Second),
		docproc.WithLeaseTimeout(time.Minute),
	)
	b := f.createBatch(t, "BATCH-LEASE", 2)
	ctx := context.Background()
	stuck, waiting := b.Documents[0].ID, b.Documents[1].ID

	if _, err := f.repo.ClaimDocument(ctx, stuck, clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.ProcessDocument(ctx, waiting); err != nil {
		t.Fatal(err)
	}

	if n, _ := f.svc.RecoverStale(ctx); n != 0 {
		t.Fatalf("before the lease expires: recovered %d, want 0", n)
	}
	clock.Advance(time.Minute + time.Second)

	n, err := f.svc.RecoverStale(ctx)
	if err != nil || n != 1 {
		t.Fatalf("RecoverStale = %d, %v; want 1", n, err)
	}
	if got := f.doc(t, stuck); got.Status != docproc.StatusPending || got.AttemptCount != 1 {
		t.Errorf("stuck doc = %s attempt %d, want PENDING attempt 1", got.Status, got.AttemptCount)
	}
	if got := f.doc(t, waiting).Status; got != docproc.StatusProcessing {
		t.Errorf("doc waiting for its callback = %s, want still PROCESSING", got)
	}
}
