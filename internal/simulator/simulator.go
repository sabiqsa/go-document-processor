// Package simulator is a fake Principal for running the server locally
// without a real one: it accepts Submits (rejecting some at random) and
// sends callbacks a little later, sometimes twice.
package simulator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/sabiqsa/go-document-processor/docproc"
)

type CallbackFunc func(context.Context, docproc.CallbackRequest) error

type Config struct {
	RejectRate    float64 // probability that Submit is rejected
	FailRate      float64 // probability that the callback reports FAILED
	DuplicateRate float64 // probability that the callback is sent twice
	MaxDelay      time.Duration
}

type Principal struct {
	cfg   Config
	log   *slog.Logger
	queue chan docproc.Document
	cb    CallbackFunc
}

func New(cfg Config, log *slog.Logger) *Principal {
	return &Principal{cfg: cfg, log: log, queue: make(chan docproc.Document, 1024)}
}

// SetCallback is called once before Run (the Service and the simulator need
// each other, so they are wired together after both exist).
func (p *Principal) SetCallback(cb CallbackFunc) { p.cb = cb }

func (p *Principal) Submit(ctx context.Context, doc docproc.Document) error {
	select {
	case <-time.After(time.Duration(rand.Int64N(int64(50 * time.Millisecond)))):
	case <-ctx.Done():
		return ctx.Err()
	}
	if rand.Float64() < p.cfg.RejectRate {
		return errors.New("simulated principal: 503 service unavailable")
	}
	select {
	case p.queue <- doc:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return errors.New("simulated principal: queue full")
	}
}

// Run sends callbacks until ctx is cancelled.
func (p *Principal) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case doc := <-p.queue:
			delay := time.Duration(rand.Int64N(int64(p.cfg.MaxDelay) + 1))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
			p.emit(ctx, doc)
		}
	}
}

func (p *Principal) emit(ctx context.Context, doc docproc.Document) {
	cb := docproc.CallbackRequest{
		CallbackID: fmt.Sprintf("sim-%s-%d", doc.ID, doc.AttemptCount),
		DocumentID: doc.ID,
		Status:     docproc.StatusSuccess,
	}
	if rand.Float64() < p.cfg.FailRate {
		cb.Status, cb.Message = docproc.StatusFailed, "simulated rejection by principal"
	}
	n := 1
	if rand.Float64() < p.cfg.DuplicateRate {
		n = 2
	}
	for range n {
		if err := p.cb(ctx, cb); err != nil {
			p.log.Error("simulator callback failed", "callback_id", cb.CallbackID, "err", err)
		}
	}
}
