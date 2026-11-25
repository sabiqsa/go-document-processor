package docproc_test

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkProcessBatch measures the overhead of the service plus the
// in-memory repository (client with no latency). It is not real throughput
// to the Principal, but the per-document cost of claim/semaphore/update.
func BenchmarkProcessBatch(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("docs=%d", size), func(b *testing.B) {
			ctx := context.Background()
			docs := makeDocs(size)
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				b.StopTimer()
				f := newFixture(nil)
				batchNo := fmt.Sprintf("BENCH-%d", i)
				i++
				if _, err := f.svc.CreateBatch(ctx, batchNo, docs); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				if err := f.svc.ProcessBatch(ctx, batchNo); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
