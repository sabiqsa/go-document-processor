// Package repotest holds the contract tests for docproc.Repository.
//
// The same tests run against the memory and postgres implementations, so if
// both pass, they behave the same from the Service's point of view.
package repotest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sabiqsa/go-document-processor/docproc"
)

// Factory creates an empty repository for one test.
type Factory func(t *testing.T) docproc.Repository

// t0 is a fixed reference time, so results do not depend on the machine clock.
const t0Unix = 1767258000 // 2026-01-01 09:00:00 UTC

func Run(t *testing.T, newRepo Factory) {
	tests := []struct {
		name string
		fn   func(*testing.T, docproc.Repository)
	}{
		{"CreateBatchIdempotent", testCreateBatchIdempotent},
		{"GetBatchNotFound", testGetBatchNotFound},
		{"ClaimIsAtomic", testClaimIsAtomic},
		{"ClaimRules", testClaimRules},
		{"FencingByAttempt", testFencingByAttempt},
		{"ReleaseClaim", testReleaseClaim},
		{"CallbackIdempotentUnderConcurrency", testCallbackConcurrent},
		{"CallbackTransitions", testCallbackTransitions},
		{"ResetRetryable", testResetRetryable},
		{"CountByStatus", testCountByStatus},
		{"RecoverStale", testRecoverStale},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { tt.fn(t, newRepo(t)) })
	}
}

func docs(n int) []docproc.Document {
	out := make([]docproc.Document, n)
	for i := range out {
		out[i] = docproc.Document{DocumentNo: fmt.Sprintf("D-%02d", i+1), Payload: fmt.Appendf(nil, `{"i":%d}`, i)}
	}
	return out
}

func seed(t *testing.T, r docproc.Repository, batchNo string, n int) *docproc.Batch {
	t.Helper()
	b, created, err := r.CreateBatch(context.Background(), batchNo, docs(n))
	if err != nil || !created {
		t.Fatalf("CreateBatch: created=%v err=%v", created, err)
	}
	return b
}

func get(t *testing.T, r docproc.Repository, id string) docproc.Document {
	t.Helper()
	d, err := r.GetDocument(context.Background(), id)
	if err != nil {
		t.Fatalf("GetDocument(%s): %v", id, err)
	}
	return d
}

func claim(t *testing.T, r docproc.Repository, id string) docproc.Document {
	t.Helper()
	d, err := r.ClaimDocument(context.Background(), id, t0())
	if err != nil {
		t.Fatalf("ClaimDocument(%s): %v", id, err)
	}
	return d
}

func testCreateBatchIdempotent(t *testing.T, r docproc.Repository) {
	ctx := context.Background()
	var createdCount atomic.Int32
	ids := make([]string, 8)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Go(func() {
			b, created, err := r.CreateBatch(ctx, "B-IDEM", docs(3))
			if err != nil {
				t.Error(err)
				return
			}
			if created {
				createdCount.Add(1)
			}
			ids[i] = b.ID
			if len(b.Documents) != 3 {
				t.Errorf("documents = %d, want 3", len(b.Documents))
			}
		})
	}
	wg.Wait()
	if createdCount.Load() != 1 {
		t.Fatalf("created=true %d times, want 1", createdCount.Load())
	}
	for _, id := range ids[1:] {
		if id != ids[0] {
			t.Fatalf("different batch IDs: %v", ids)
		}
	}
	b, _, _ := r.CreateBatch(ctx, "B-IDEM", docs(3))
	for i, d := range b.Documents {
		if d.Status != docproc.StatusPending || d.AttemptCount != 0 || d.DocumentNo != fmt.Sprintf("D-%02d", i+1) {
			t.Errorf("doc %d = %+v", i, d)
		}
	}
}

func testGetBatchNotFound(t *testing.T, r docproc.Repository) {
	if _, err := r.GetBatch(context.Background(), "missing"); !errors.Is(err, docproc.ErrBatchNotFound) {
		t.Fatalf("err = %v", err)
	}
	if _, err := r.GetDocument(context.Background(), "424242"); !errors.Is(err, docproc.ErrDocumentNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func testClaimIsAtomic(t *testing.T, r docproc.Repository) {
	id := seed(t, r, "B-CLAIM", 1).Documents[0].ID
	var wins, notClaimable atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			<-start
			_, err := r.ClaimDocument(context.Background(), id, t0())
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, docproc.ErrNotClaimable):
				notClaimable.Add(1)
			default:
				t.Errorf("unexpected: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
	if wins.Load() != 1 || notClaimable.Load() != 31 {
		t.Fatalf("wins=%d notClaimable=%d, want 1/31", wins.Load(), notClaimable.Load())
	}
	if d := get(t, r, id); d.Status != docproc.StatusProcessing || d.AttemptCount != 1 {
		t.Fatalf("doc = %s/%d", d.Status, d.AttemptCount)
	}
}

func testClaimRules(t *testing.T, r docproc.Repository) {
	ctx := context.Background()
	id := seed(t, r, "B-RULES", 1).Documents[0].ID

	if _, err := r.ClaimDocument(ctx, "999999", t0()); !errors.Is(err, docproc.ErrDocumentNotFound) {
		t.Errorf("claim missing: %v", err)
	}
	for attempt := 1; attempt <= docproc.MaxAttempts; attempt++ {
		d := claim(t, r, id)
		if d.AttemptCount != attempt {
			t.Fatalf("attempt = %d, want %d", d.AttemptCount, attempt)
		}
		if err := r.MarkFailed(ctx, id, attempt, "boom", nil, t0()); err != nil {
			t.Fatal(err)
		}
		// A FAILED document must not be claimable directly.
		if _, err := r.ClaimDocument(ctx, id, t0()); !errors.Is(err, docproc.ErrNotClaimable) {
			t.Fatalf("claim FAILED: %v", err)
		}
		if _, err := r.ResetRetryable(ctx, d.BatchID, t0()); err != nil {
			t.Fatal(err)
		}
	}
	if d := get(t, r, id); d.Status != docproc.StatusFailed || d.AttemptCount != docproc.MaxAttempts {
		t.Fatalf("after attempts ran out = %s/%d, want FAILED/3", d.Status, d.AttemptCount)
	}
}

func testFencingByAttempt(t *testing.T, r docproc.Repository) {
	ctx := context.Background()
	id := seed(t, r, "B-FENCE", 1).Documents[0].ID
	claim(t, r, id)

	// A worker with the wrong attempt must not change anything.
	if err := r.MarkFailed(ctx, id, 7, "stale", nil, t0()); !errors.Is(err, docproc.ErrStaleAttempt) {
		t.Fatalf("stale MarkFailed: err = %v, want ErrStaleAttempt", err)
	}
	if d := get(t, r, id); d.Status != docproc.StatusProcessing {
		t.Fatalf("stale MarkFailed changed status to %s", d.Status)
	}

	// The SUCCESS callback arrives first, then the worker reports back.
	if _, err := r.ApplyCallback(ctx, docproc.CallbackRequest{CallbackID: "f-1", DocumentID: id, Status: docproc.StatusSuccess}, t0(), nil); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkFailed(ctx, id, 1, "late", nil, t0()); !errors.Is(err, docproc.ErrStaleAttempt) {
		t.Fatalf("late MarkFailed: err = %v, want ErrStaleAttempt", err)
	}
	if err := r.MarkSubmitted(ctx, id, 1, t0()); !errors.Is(err, docproc.ErrStaleAttempt) {
		t.Fatalf("late MarkSubmitted: err = %v, want ErrStaleAttempt", err)
	}
	if err := r.ReleaseClaim(ctx, id, 1, "late", t0()); !errors.Is(err, docproc.ErrStaleAttempt) {
		t.Fatalf("late ReleaseClaim: err = %v, want ErrStaleAttempt", err)
	}
	if d := get(t, r, id); d.Status != docproc.StatusSuccess {
		t.Fatalf("SUCCESS was overwritten with %s", d.Status)
	}
	if err := r.MarkFailed(ctx, "999999", 1, "x", nil, t0()); !errors.Is(err, docproc.ErrDocumentNotFound) {
		t.Fatalf("MarkFailed missing: %v", err)
	}
}

func testReleaseClaim(t *testing.T, r docproc.Repository) {
	ctx := context.Background()
	b := seed(t, r, "B-REL", 1)
	id := b.Documents[0].ID

	claim(t, r, id)
	if err := r.ReleaseClaim(ctx, id, 1, "cancelled", t0()); err != nil {
		t.Fatal(err)
	}
	if d := get(t, r, id); d.Status != docproc.StatusPending || d.AttemptCount != 1 {
		t.Fatalf("after release = %s/%d, want PENDING/1", d.Status, d.AttemptCount)
	}

	// Releasing the last attempt -> FAILED, not a PENDING that can never be claimed.
	claim(t, r, id)
	if err := r.ReleaseClaim(ctx, id, 2, "cancelled", t0()); err != nil {
		t.Fatal(err)
	}
	claim(t, r, id)
	if err := r.ReleaseClaim(ctx, id, 3, "cancelled", t0()); err != nil {
		t.Fatal(err)
	}
	if d := get(t, r, id); d.Status != docproc.StatusFailed {
		t.Fatalf("release on last attempt = %s, want FAILED", d.Status)
	}
}

func testCallbackConcurrent(t *testing.T, r docproc.Repository) {
	id := seed(t, r, "B-CBC", 1).Documents[0].ID
	claim(t, r, id)

	var fresh, dup atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 24 {
		wg.Go(func() {
			<-start
			out, err := r.ApplyCallback(context.Background(),
				docproc.CallbackRequest{CallbackID: "same", DocumentID: id, Status: docproc.StatusSuccess}, t0(), nil)
			if err != nil {
				t.Error(err)
				return
			}
			if out.Duplicate {
				dup.Add(1)
			} else if out.Applied {
				fresh.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()
	if fresh.Load() != 1 || dup.Load() != 23 {
		t.Fatalf("fresh=%d dup=%d, want 1/23", fresh.Load(), dup.Load())
	}
}

func testCallbackTransitions(t *testing.T, r docproc.Repository) {
	ctx := context.Background()
	b := seed(t, r, "B-CBT", 2)
	a, c := b.Documents[0].ID, b.Documents[1].ID
	apply := func(cbID, docID string, st docproc.DocStatus) docproc.CallbackOutcome {
		t.Helper()
		out, err := r.ApplyCallback(ctx, docproc.CallbackRequest{CallbackID: cbID, DocumentID: docID, Status: st, Message: "m"}, t0(),
			func(int) time.Duration { return time.Minute })
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	claim(t, r, a)
	if out := apply("t1", a, docproc.StatusFailed); !out.Applied || out.Status != docproc.StatusFailed {
		t.Fatalf("PROCESSING+FAILED: %+v", out)
	}
	d := get(t, r, a)
	if d.NextRetryAt == nil || !d.NextRetryAt.Equal(t0().Add(time.Minute)) || d.LastError == nil {
		t.Fatalf("a FAILED callback must set next_retry_at and last_error: %+v", d)
	}
	if out := apply("t2", a, docproc.StatusSuccess); !out.Applied || out.Status != docproc.StatusSuccess {
		t.Fatalf("FAILED+SUCCESS: %+v", out)
	}
	if out := apply("t3", a, docproc.StatusFailed); out.Applied || out.Status != docproc.StatusSuccess {
		t.Fatalf("SUCCESS+FAILED must be ignored: %+v", out)
	}
	if out := apply("t4", c, docproc.StatusFailed); out.Applied || out.Status != docproc.StatusPending {
		t.Fatalf("PENDING+FAILED must be ignored: %+v", out)
	}
	if _, err := r.ApplyCallback(ctx, docproc.CallbackRequest{CallbackID: "t5", DocumentID: "999999", Status: docproc.StatusSuccess}, t0(), nil); !errors.Is(err, docproc.ErrDocumentNotFound) {
		t.Fatalf("missing doc: %v", err)
	}
}

func testResetRetryable(t *testing.T, r docproc.Repository) {
	ctx := context.Background()
	b := seed(t, r, "B-RESET", 3)
	now := b.Documents[0].ID
	later := b.Documents[1].ID
	untouched := b.Documents[2].ID

	claim(t, r, now)
	_ = r.MarkFailed(ctx, now, 1, "x", nil, t0())
	claim(t, r, later)
	future := t0().Add(time.Hour)
	_ = r.MarkFailed(ctx, later, 1, "x", &future, t0())

	n, err := r.ResetRetryable(ctx, b.ID, t0())
	if err != nil || n != 1 {
		t.Fatalf("ResetRetryable = %d, %v; want 1", n, err)
	}
	if get(t, r, now).Status != docproc.StatusPending || get(t, r, later).Status != docproc.StatusFailed {
		t.Fatal("only documents whose delay has passed may be reset")
	}
	if n, _ := r.ResetRetryable(ctx, b.ID, future); n != 1 {
		t.Fatalf("after the delay = %d, want 1", n)
	}
	if get(t, r, untouched).Status != docproc.StatusPending {
		t.Fatal("an unrelated document changed")
	}
}

func testCountByStatus(t *testing.T, r docproc.Repository) {
	ctx := context.Background()
	b := seed(t, r, "B-COUNT", 5)
	ids := make([]string, len(b.Documents))
	for i, d := range b.Documents {
		ids[i] = d.ID
	}
	claim(t, r, ids[0]) // PROCESSING
	claim(t, r, ids[1])
	_ = r.MarkFailed(ctx, ids[1], 1, "x", nil, t0()) // FAILED, retryable
	claim(t, r, ids[2])
	_, _ = r.ApplyCallback(ctx, docproc.CallbackRequest{CallbackID: "cnt", DocumentID: ids[2], Status: docproc.StatusSuccess}, t0(), nil)
	claim(t, r, ids[3])
	_ = r.ReleaseClaim(ctx, ids[3], 1, "x", t0()) // PENDING, but already attempted

	sum, err := r.CountByStatus(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[docproc.DocStatus]int{
		docproc.StatusPending: 2, docproc.StatusProcessing: 1,
		docproc.StatusSuccess: 1, docproc.StatusFailed: 1,
	}
	for st, n := range want {
		if sum.Counts[st] != n {
			t.Errorf("%s = %d, want %d", st, sum.Counts[st], n)
		}
	}
	if sum.Total != 5 || sum.Retryable != 1 || sum.Untouched != 1 {
		t.Errorf("summary = %+v", sum)
	}
}

func testRecoverStale(t *testing.T, r docproc.Repository) {
	ctx := context.Background()
	b := seed(t, r, "B-STALE", 3)
	crashed, submitted, fresh := b.Documents[0].ID, b.Documents[1].ID, b.Documents[2].ID

	claim(t, r, crashed)
	claim(t, r, submitted)
	_ = r.MarkSubmitted(ctx, submitted, 1, t0())
	if _, err := r.ClaimDocument(ctx, fresh, t0().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}

	n, err := r.RecoverStale(ctx, t0().Add(5*time.Minute), t0().Add(10*time.Minute))
	if err != nil || n != 1 {
		t.Fatalf("RecoverStale = %d, %v; want 1", n, err)
	}
	if get(t, r, crashed).Status != docproc.StatusPending {
		t.Error("stuck document was not returned to PENDING")
	}
	if get(t, r, submitted).Status != docproc.StatusProcessing {
		t.Error("a document waiting for its callback was taken over")
	}
	if get(t, r, fresh).Status != docproc.StatusProcessing {
		t.Error("a document with a valid lease was taken over")
	}
}

func t0() time.Time { return time.Unix(t0Unix, 0).UTC() }
