// Package httpapi exposes the Service over HTTP: the callback endpoint for
// the Principal and a few operational endpoints for batches.
package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"sync"

	"github.com/sabiqsa/go-document-processor/docproc"
)

const (
	maxCallbackBody = 64 << 10 // callbacks are small; cap them so large bodies cannot flood us
	maxBatchBody    = 32 << 20
	SignatureHeader = "X-Principal-Signature"
)

type Config struct {
	// CallbackSecret is used to verify the HMAC-SHA256 of the callback body.
	// Empty disables verification (local development only; the server
	// refuses to start that way against a real Principal).
	CallbackSecret string
	Logger         *slog.Logger
}

type Handler struct {
	svc    *docproc.Service
	secret []byte
	log    *slog.Logger
	runner *Runner
	mux    *http.ServeMux
}

func New(svc *docproc.Service, runner *Runner, cfg Config) *Handler {
	h := &Handler{svc: svc, secret: []byte(cfg.CallbackSecret), log: cfg.Logger, runner: runner, mux: http.NewServeMux()}
	if h.log == nil {
		h.log = slog.New(slog.DiscardHandler)
	}
	h.mux.HandleFunc("POST /v1/callbacks", h.handleCallback)
	h.mux.HandleFunc("POST /v1/batches", h.createBatch)
	h.mux.HandleFunc("GET /v1/batches/{batchNo}", h.batchStatus)
	h.mux.HandleFunc("POST /v1/batches/{batchNo}/process", h.processBatch)
	h.mux.HandleFunc("POST /v1/batches/{batchNo}/retry", h.retryFailed)
	h.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	h.mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, h.svc.Stats()) })
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

type callbackBody struct {
	CallbackID string `json:"callback_id"`
	DocumentID string `json:"document_id"`
	Status     string `json:"status"`
	Message    string `json:"message"`
}

// handleCallback
//
//	200 accepted (duplicates included; the Principal does not need to know)
//	400 body is not valid JSON
//	401 signature missing or wrong
//	404 unknown document_id
//	413 body too large
//	415 content type is not JSON
//	422 required field empty / invalid status
//	500 internal error: the Principal should retry, which is safe because
//	    the handler is idempotent
func (h *Handler) handleCallback(w http.ResponseWriter, r *http.Request) {
	if !isJSON(r) {
		writeError(w, http.StatusUnsupportedMediaType, "content-type must be application/json")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxCallbackBody))
	if err != nil {
		if isTooLarge(err) {
			writeError(w, http.StatusRequestEntityTooLarge, "body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "cannot read body")
		return
	}
	if !h.validSignature(r.Header.Get(SignatureHeader), raw) {
		writeError(w, http.StatusUnauthorized, "invalid signature")
		return
	}

	var body callbackBody
	if err := json.Unmarshal(raw, &body); err != nil {
		writeError(w, http.StatusBadRequest, "malformed JSON")
		return
	}
	cb := docproc.CallbackRequest{
		CallbackID: strings.TrimSpace(body.CallbackID),
		DocumentID: strings.TrimSpace(body.DocumentID),
		Status:     docproc.DocStatus(strings.ToUpper(strings.TrimSpace(body.Status))),
		Message:    body.Message,
	}
	if err := h.svc.HandleCallback(r.Context(), cb); err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"result": "accepted"})
}

func (h *Handler) validSignature(header string, body []byte) bool {
	if len(h.secret) == 0 {
		return true
	}
	got, ok := strings.CutPrefix(header, "sha256=")
	if !ok {
		return false
	}
	sig, err := hex.DecodeString(got)
	if err != nil {
		return false
	}
	return hmac.Equal(sig, Sign(h.secret, body))
}

// Sign returns the HMAC-SHA256 of body. Exported for clients and tests.
func Sign(secret, body []byte) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	return m.Sum(nil)
}

type createBatchBody struct {
	BatchNo   string `json:"batch_no"`
	Documents []struct {
		DocumentNo string          `json:"document_no"`
		Payload    json.RawMessage `json:"payload"`
	} `json:"documents"`
}

func (h *Handler) createBatch(w http.ResponseWriter, r *http.Request) {
	if !isJSON(r) {
		writeError(w, http.StatusUnsupportedMediaType, "content-type must be application/json")
		return
	}
	var body createBatchBody
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBatchBody))
	if err := dec.Decode(&body); err != nil {
		if isTooLarge(err) {
			writeError(w, http.StatusRequestEntityTooLarge, "body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "malformed JSON")
		return
	}
	docs := make([]docproc.Document, len(body.Documents))
	for i, d := range body.Documents {
		docs[i] = docproc.Document{DocumentNo: d.DocumentNo, Payload: d.Payload}
	}
	b, err := h.svc.CreateBatch(r.Context(), body.BatchNo, docs)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	type docOut struct {
		ID         string `json:"id"`
		DocumentNo string `json:"document_no"`
		Status     string `json:"status"`
	}
	out := struct {
		ID        string   `json:"id"`
		BatchNo   string   `json:"batch_no"`
		Documents []docOut `json:"documents"`
	}{ID: b.ID, BatchNo: b.BatchNo, Documents: make([]docOut, len(b.Documents))}
	for i, d := range b.Documents {
		out.Documents[i] = docOut{ID: d.ID, DocumentNo: d.DocumentNo, Status: string(d.Status)}
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) batchStatus(w http.ResponseWriter, r *http.Request) {
	st, err := h.svc.GetBatchStatus(r.Context(), r.PathValue("batchNo"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"batch_no":  st.BatchNo,
		"state":     st.State,
		"done":      st.Done,
		"total":     st.Total,
		"retryable": st.Retryable,
		"counts":    st.Counts,
	})
}

// processBatch runs ProcessBatch in the background (it can take a while)
// and answers 202 right away. Progress is available at
// GET /v1/batches/{batchNo}.
func (h *Handler) processBatch(w http.ResponseWriter, r *http.Request) {
	batchNo := r.PathValue("batchNo")
	if _, err := h.svc.GetBatchStatus(r.Context(), batchNo); err != nil {
		h.fail(w, r, err)
		return
	}
	started := h.runner.Start(batchNo, func(ctx context.Context) error {
		return h.svc.ProcessBatch(ctx, batchNo)
	})
	if !started {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batch is already being processed"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"result": "processing started"})
}

func (h *Handler) retryFailed(w http.ResponseWriter, r *http.Request) {
	n, err := h.svc.RetryFailed(r.Context(), r.PathValue("batchNo"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"retried": n})
}

func (h *Handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, docproc.ErrInvalidInput), errors.Is(err, docproc.ErrInvalidCallback):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, docproc.ErrBatchNotFound), errors.Is(err, docproc.ErrDocumentNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, docproc.ErrBatchConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusServiceUnavailable, "request cancelled")
	default:
		h.log.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func isTooLarge(err error) bool {
	var tooBig *http.MaxBytesError
	return errors.As(err, &tooBig)
}

func isJSON(r *http.Request) bool {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && mt == "application/json"
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// Runner runs background work (ProcessBatch) that stops when the server
// shuts down, and keeps the same batch from being processed twice at the
// same time from this instance.
type Runner struct {
	ctx     context.Context
	log     *slog.Logger
	wg      sync.WaitGroup
	mu      sync.Mutex
	running map[string]struct{}
}

func NewRunner(ctx context.Context, log *slog.Logger) *Runner {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Runner{ctx: ctx, log: log, running: make(map[string]struct{})}
}

func (r *Runner) Start(key string, fn func(context.Context) error) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, busy := r.running[key]; busy || r.ctx.Err() != nil {
		return false
	}
	r.running[key] = struct{}{}
	r.wg.Go(func() {
		defer func() {
			r.mu.Lock()
			delete(r.running, key)
			r.mu.Unlock()
		}()
		if err := fn(r.ctx); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Error("background job failed", "key", key, "err", err)
		}
	})
	return true
}

// Wait waits for every job to finish. Call it after the Runner's ctx is cancelled.
func (r *Runner) Wait() { r.wg.Wait() }
