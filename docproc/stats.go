package docproc

import "sync/atomic"

// Stats is a snapshot of operational counters. It is deliberately simple
// (atomic counters) so the package does not depend on a specific metrics
// library; the HTTP layer can expose it as is or map it to Prometheus.
type Stats struct {
	SubmitCalls int64
	// SubmitFailures counts failed Submits that were recorded as FAILED. A
	// failure that arrives after a callback already settled the document is
	// not counted, because nothing failed from the document's point of view.
	SubmitFailures     int64
	InFlight           int64
	CallbacksApplied   int64
	CallbacksIgnored   int64 // new callback that did not change the status (e.g. a late FAILED)
	CallbacksDuplicate int64
	Recovered          int64
}

type counters struct {
	submitCalls, submitFailures, inFlight        atomic.Int64
	cbApplied, cbIgnored, cbDuplicate, recovered atomic.Int64
}

func (s *Service) Stats() Stats {
	c := &s.counters
	return Stats{
		SubmitCalls:        c.submitCalls.Load(),
		SubmitFailures:     c.submitFailures.Load(),
		InFlight:           c.inFlight.Load(),
		CallbacksApplied:   c.cbApplied.Load(),
		CallbacksIgnored:   c.cbIgnored.Load(),
		CallbacksDuplicate: c.cbDuplicate.Load(),
		Recovered:          c.recovered.Load(),
	}
}
