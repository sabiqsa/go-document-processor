package principal_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sabiqsa/go-document-processor/docproc"
	"github.com/sabiqsa/go-document-processor/principal"
)

func TestClientSubmit(t *testing.T) {
	keys := make(chan string, 2) // a channel, not a plain variable: the handler runs on another goroutine
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys <- r.Header.Get("Idempotency-Key")
		if r.Header.Get("X-Mode") == "reject" {
			http.Error(w, "overloaded", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := principal.NewClient(srv.URL, nil)
	doc := docproc.Document{ID: "42", DocumentNo: "A", Payload: []byte(`{}`), AttemptCount: 2}
	if err := c.Submit(context.Background(), doc); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got := <-keys; got != "doc-42-attempt-2" {
		t.Fatalf("Idempotency-Key = %q", got)
	}

	reject := principal.NewClient(srv.URL, &http.Client{Transport: headerRT{"X-Mode", "reject"}})
	if err := reject.Submit(context.Background(), doc); err == nil {
		t.Fatal("a 503 response must be an error")
	}
}

func TestClientSubmit_RespectsContext(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(block)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := principal.NewClient(srv.URL, nil).Submit(ctx, docproc.Document{ID: "1", Payload: []byte(`{}`)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want DeadlineExceeded", err)
	}
}

type headerRT struct{ k, v string }

func (h headerRT) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set(h.k, h.v)
	return http.DefaultTransport.RoundTrip(r)
}
