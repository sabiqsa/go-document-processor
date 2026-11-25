package docproc_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/sabiqsa/go-document-processor/docproc"
)

// Test point 1: never more than 3 concurrent Submits.
//
// Every Submit is held at the gate. synctest.Wait() only returns once every
// goroutine in the bubble is durably blocked, so at that moment the number
// of running Submits is final: if the limiter leaked, it would be above 3.
// No guessing at sleep durations.
func TestProcessBatch_MaxConcurrentSubmit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newFakePrincipal()
		client.gate = make(chan struct{})
		f := newFixture(client)
		b := f.createBatch(t, "BATCH-CONC", 20)

		done := make(chan error, 1)
		go func() { done <- f.svc.ProcessBatch(context.Background(), "BATCH-CONC") }()

		synctest.Wait()
		if got := client.inFlight.Load(); got != docproc.MaxConcurrent {
			t.Fatalf("in-flight while workers pile up = %d, want %d", got, docproc.MaxConcurrent)
		}

		close(client.gate)
		if err := <-done; err != nil {
			t.Fatalf("ProcessBatch: %v", err)
		}
		if got := client.maxInFlight.Load(); got != docproc.MaxConcurrent {
			t.Errorf("maxInFlight = %d, want %d", got, docproc.MaxConcurrent)
		}
		for _, d := range b.Documents {
			if c := client.callsFor(d.ID); c != 1 {
				t.Errorf("doc %s: Submit called %d times, want 1", d.DocumentNo, c)
			}
		}
		if st := f.status(t, "BATCH-CONC"); st.Counts[docproc.StatusProcessing] != 20 {
			t.Errorf("PROCESSING = %d, want 20", st.Counts[docproc.StatusProcessing])
		}
	})
}

// The limit of 3 must be global per Service: two parallel ProcessBatch calls
// plus direct ProcessDocument calls still total at most 3.
func TestConcurrencyLimitIsGlobalAcrossBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newFakePrincipal()
		client.gate = make(chan struct{})
		f := newFixture(client)
		f.createBatch(t, "A", 10)
		f.createBatch(t, "B", 10)
		c := f.createBatch(t, "C", 2)

		var wg sync.WaitGroup
		errs := make(chan error, 4)
		for _, no := range []string{"A", "B"} {
			wg.Go(func() { errs <- f.svc.ProcessBatch(context.Background(), no) })
		}
		for _, d := range c.Documents {
			wg.Go(func() { errs <- f.svc.ProcessDocument(context.Background(), d.ID) })
		}

		synctest.Wait()
		if got := client.inFlight.Load(); got != docproc.MaxConcurrent {
			t.Fatalf("in-flight across batches = %d, want %d", got, docproc.MaxConcurrent)
		}
		close(client.gate)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}
		if got := client.maxInFlight.Load(); got > docproc.MaxConcurrent {
			t.Errorf("maxInFlight = %d, want <= %d", got, docproc.MaxConcurrent)
		}
		if got := client.totalCalls(); got != 22 {
			t.Errorf("total Submits = %d, want 22", got)
		}
	})
}

// Test point 3: ProcessDocument for the same ID from many goroutines at once
// results in exactly one Submit.
func TestProcessDocument_SameIDManyGoroutines(t *testing.T) {
	f := newFixture(nil)
	b := f.createBatch(t, "BATCH-DUP", 1)
	id := b.Documents[0].ID

	const workers = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			<-start
			if err := f.svc.ProcessDocument(context.Background(), id); err != nil {
				t.Errorf("ProcessDocument: %v", err)
			}
		})
	}
	close(start) // release every goroutine at once
	wg.Wait()

	if c := f.client.callsFor(id); c != 1 {
		t.Fatalf("Submit called %d times, want 1", c)
	}
	d := f.doc(t, id)
	if d.Status != docproc.StatusProcessing || d.AttemptCount != 1 {
		t.Fatalf("doc = %s attempt %d, want PROCESSING attempt 1", d.Status, d.AttemptCount)
	}
}

// Test point 7: cancellation in the middle of ProcessBatch.
func TestProcessBatch_ContextCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newFakePrincipal()
		client.gate = make(chan struct{}) // never opened: Submit only ends through ctx
		f := newFixture(client)
		b := f.createBatch(t, "BATCH-CANCEL", 20)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- f.svc.ProcessBatch(ctx, "BATCH-CANCEL") }()

		synctest.Wait() // 3 documents in Submit, the rest waiting
		cancel()

		err := <-done
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ProcessBatch error = %v, want context.Canceled", err)
		}

		st := f.status(t, "BATCH-CANCEL")
		if st.Counts[docproc.StatusProcessing] != 0 {
			t.Fatalf("%d documents still PROCESSING after cancel", st.Counts[docproc.StatusProcessing])
		}
		if st.Counts[docproc.StatusPending] != 20 {
			t.Fatalf("PENDING = %d, want 20 (all back in the queue)", st.Counts[docproc.StatusPending])
		}
		attempted := 0
		for _, d := range b.Documents {
			if f.doc(t, d.ID).AttemptCount > 0 {
				attempted++
			}
		}
		if attempted != docproc.MaxConcurrent {
			t.Errorf("documents attempted = %d, want %d", attempted, docproc.MaxConcurrent)
		}

		// After the cancel, the batch can carry on normally.
		close(client.gate)
		if err := f.svc.ProcessBatch(context.Background(), "BATCH-CANCEL"); err != nil {
			t.Fatalf("ProcessBatch again: %v", err)
		}
		if got := f.status(t, "BATCH-CANCEL").Counts[docproc.StatusProcessing]; got != 20 {
			t.Errorf("PROCESSING after resuming = %d, want 20", got)
		}
	})
	// Goroutine leaks: synctest.Test fails (deadlock) if a goroutine in the
	// bubble is still blocked when the function returns, and goleak in
	// TestMain checks again at the end of the package.
}

func TestProcessBatch_AlreadyCancelledContext(t *testing.T) {
	f := newFixture(nil)
	f.createBatch(t, "BATCH-X", 5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := f.svc.ProcessBatch(ctx, "BATCH-X"); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if n := f.client.totalCalls(); n != 0 {
		t.Fatalf("Submit called %d times although ctx was already cancelled", n)
	}
}

// Test point 6: every operation running at the same time. The test checks
// invariants, not ordering, so it stays stable whatever the goroutine
// schedule. Its main value is when run with -race.
func TestMixedOperations_NoRace(t *testing.T) {
	client := newFakePrincipal()
	// Fail deterministically for some document+attempt combinations.
	client.failFn = func(d docproc.Document) error {
		n, _ := strconv.Atoi(d.ID)
		if (n+d.AttemptCount)%3 == 0 {
			return errFail(d)
		}
		return nil
	}
	f := newFixture(client)

	batches := []string{"MIX-1", "MIX-2", "MIX-3"}
	var all []docproc.Document
	for _, no := range batches {
		all = append(all, f.createBatch(t, no, 30).Documents...)
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	stopPolling := make(chan struct{})

	for _, no := range batches {
		wg.Go(func() {
			for range docproc.MaxAttempts {
				if err := f.svc.ProcessBatch(ctx, no); err != nil {
					t.Errorf("ProcessBatch(%s): %v", no, err)
				}
				if _, err := f.svc.RetryFailed(ctx, no); err != nil {
					t.Errorf("RetryFailed(%s): %v", no, err)
				}
			}
		})
	}

	// Callbacks arrive concurrently, including duplicates sent in parallel.
	for i, d := range all {
		wg.Go(func() {
			st := docproc.StatusSuccess
			if i%4 == 0 {
				st = docproc.StatusFailed
			}
			cb := docproc.CallbackRequest{CallbackID: "cb-" + d.ID, DocumentID: d.ID, Status: st}
			for range 3 {
				if err := f.svc.HandleCallback(ctx, cb); err != nil {
					t.Errorf("HandleCallback: %v", err)
				}
			}
		})
	}

	var pollWG sync.WaitGroup
	for _, no := range batches {
		pollWG.Go(func() {
			for {
				select {
				case <-stopPolling:
					return
				default:
				}
				if _, err := f.svc.GetBatchStatus(ctx, no); err != nil {
					t.Errorf("GetBatchStatus(%s): %v", no, err)
					return
				}
			}
		})
	}

	wg.Wait()
	close(stopPolling)
	pollWG.Wait()

	total := 0
	for _, no := range batches {
		st := f.status(t, no)
		sum := 0
		for _, c := range st.Counts {
			sum += c
		}
		if sum != st.Total || st.Total != 30 {
			t.Errorf("%s: per-status sum %d, total %d, want 30", no, sum, st.Total)
		}
		total += st.Total
	}
	for _, d := range all {
		got := f.doc(t, d.ID)
		if got.AttemptCount > docproc.MaxAttempts {
			t.Errorf("doc %s attempt %d exceeds the limit", d.ID, got.AttemptCount)
		}
		if c := client.callsFor(d.ID); c != got.AttemptCount {
			t.Errorf("doc %s: %d Submits, attempt_count %d", d.ID, c, got.AttemptCount)
		}
		if n := len(f.repo.Callbacks(d.ID)); n != 1 {
			t.Errorf("doc %s: %d callback records, want 1", d.ID, n)
		}
	}
	if total != len(all) {
		t.Fatalf("total %d, want %d", total, len(all))
	}
	t.Logf("stats: %+v", f.svc.Stats())
}
