package docproc_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/sabiqsa/go-document-processor/docproc"
	"github.com/sabiqsa/go-document-processor/docproc/memory"
)

// TestMain checks that no goroutine is left running after all tests in this
// package finish.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fakePrincipal is a mock PrincipalClient.
//
//   - inFlight / maxInFlight: how many Submits are running at the same time.
//   - gate: when set, Submit blocks until gate is closed or ctx is done. This
//     is the barrier that makes workers actually pile up.
//   - failFn: decides the Submit result per document/attempt.
type fakePrincipal struct {
	inFlight    atomic.Int64
	maxInFlight atomic.Int64

	gate   chan struct{}
	failFn func(doc docproc.Document) error

	mu    sync.Mutex
	calls map[string]int
}

func newFakePrincipal() *fakePrincipal {
	return &fakePrincipal{calls: make(map[string]int)}
}

func (f *fakePrincipal) Submit(ctx context.Context, doc docproc.Document) error {
	cur := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for {
		prev := f.maxInFlight.Load()
		if cur <= prev || f.maxInFlight.CompareAndSwap(prev, cur) {
			break
		}
	}

	f.mu.Lock()
	f.calls[doc.ID]++
	f.mu.Unlock()

	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.failFn != nil {
		return f.failFn(doc)
	}
	return nil
}

func (f *fakePrincipal) callsFor(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[id]
}

func (f *fakePrincipal) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		n += c
	}
	return n
}

// manualClock is a clock that only moves when the test advances it.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock() *manualClock {
	return &manualClock{now: time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type fixture struct {
	repo   *memory.Repository
	client *fakePrincipal
	svc    *docproc.Service
}

func newFixture(client *fakePrincipal, opts ...docproc.Option) fixture {
	if client == nil {
		client = newFakePrincipal()
	}
	repo := memory.New()
	return fixture{repo: repo, client: client, svc: docproc.NewService(repo, client, opts...)}
}

func makeDocs(n int) []docproc.Document {
	docs := make([]docproc.Document, n)
	for i := range docs {
		docs[i] = docproc.Document{
			DocumentNo: fmt.Sprintf("DOC-%03d", i+1),
			Payload:    fmt.Appendf(nil, `{"seq":%d}`, i+1),
		}
	}
	return docs
}

func (f fixture) createBatch(t *testing.T, batchNo string, n int) *docproc.Batch {
	t.Helper()
	b, err := f.svc.CreateBatch(context.Background(), batchNo, makeDocs(n))
	if err != nil {
		t.Fatalf("CreateBatch(%s): %v", batchNo, err)
	}
	return b
}

func (f fixture) doc(t *testing.T, id string) docproc.Document {
	t.Helper()
	d, err := f.repo.GetDocument(context.Background(), id)
	if err != nil {
		t.Fatalf("GetDocument(%s): %v", id, err)
	}
	return d
}

func (f fixture) status(t *testing.T, batchNo string) *docproc.BatchStatus {
	t.Helper()
	st, err := f.svc.GetBatchStatus(context.Background(), batchNo)
	if err != nil {
		t.Fatalf("GetBatchStatus(%s): %v", batchNo, err)
	}
	return st
}

func errFail(doc docproc.Document) error {
	return fmt.Errorf("principal rejected %s attempt %d", doc.ID, doc.AttemptCount)
}
