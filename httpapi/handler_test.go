package httpapi_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sabiqsa/go-document-processor/docproc"
	"github.com/sabiqsa/go-document-processor/docproc/memory"
	"github.com/sabiqsa/go-document-processor/httpapi"
)

type acceptAll struct{}

func (acceptAll) Submit(context.Context, docproc.Document) error { return nil }

const secret = "s3cret"

func setup(t *testing.T) (*httptest.Server, *docproc.Service, string) {
	t.Helper()
	svc := docproc.NewService(memory.New(), acceptAll{})
	ctx, cancel := context.WithCancel(context.Background())
	runner := httpapi.NewRunner(ctx, nil)
	srv := httptest.NewServer(httpapi.New(svc, runner, httpapi.Config{CallbackSecret: secret}))
	t.Cleanup(func() {
		srv.Close()
		cancel()
		runner.Wait()
	})

	b, err := svc.CreateBatch(context.Background(), "HTTP-1", []docproc.Document{{DocumentNo: "A", Payload: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	id := b.Documents[0].ID
	if err := svc.ProcessDocument(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	return srv, svc, id
}

func post(t *testing.T, url, contentType, body, sig string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	if sig != "" {
		req.Header.Set(httpapi.SignatureHeader, sig)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func sign(body string) string {
	return "sha256=" + hex.EncodeToString(httpapi.Sign([]byte(secret), []byte(body)))
}

func TestCallbackEndpoint(t *testing.T) {
	srv, svc, id := setup(t)
	valid := `{"callback_id":"evt-1","document_id":"` + id + `","status":"SUCCESS"}`

	tests := []struct {
		name        string
		contentType string
		body        string
		sig         string
		want        int
	}{
		{"success", "application/json", valid, sign(valid), http.StatusOK},
		{"duplicate is still 200", "application/json", valid, sign(valid), http.StatusOK},
		{"wrong content type", "text/plain", valid, sign(valid), http.StatusUnsupportedMediaType},
		{"no signature", "application/json", valid, "", http.StatusUnauthorized},
		{"wrong signature", "application/json", valid, sign(valid + " "), http.StatusUnauthorized},
		{"malformed JSON", "application/json", `{"callback_id":`, sign(`{"callback_id":`), http.StatusBadRequest},
		{"invalid status", "application/json", `{"callback_id":"x","document_id":"` + id + `","status":"DONE"}`,
			sign(`{"callback_id":"x","document_id":"` + id + `","status":"DONE"}`), http.StatusUnprocessableEntity},
		{"unknown document", "application/json", `{"callback_id":"y","document_id":"999","status":"SUCCESS"}`,
			sign(`{"callback_id":"y","document_id":"999","status":"SUCCESS"}`), http.StatusNotFound},
		{"body too large", "application/json", `{"message":"` + strings.Repeat("x", 70<<10) + `"}`, "", http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := post(t, srv.URL+"/v1/callbacks", tt.contentType, tt.body, tt.sig)
			if resp.StatusCode != tt.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}

	st, _ := svc.GetBatchStatus(context.Background(), "HTTP-1")
	if st.Counts[docproc.StatusSuccess] != 1 {
		t.Fatalf("document did not become SUCCESS: %+v", st.Counts)
	}
	if s := svc.Stats(); s.CallbacksApplied != 1 || s.CallbacksDuplicate != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestCallbackEndpoint_MethodNotAllowed(t *testing.T) {
	srv, _, _ := setup(t)
	resp, err := http.Get(srv.URL + "/v1/callbacks")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

func TestBatchEndpoints(t *testing.T) {
	srv, _, _ := setup(t)

	body := `{"batch_no":"HTTP-2","documents":[{"document_no":"X","payload":{"a":1}},{"document_no":"Y","payload":{"b":2}}]}`
	if resp := post(t, srv.URL+"/v1/batches", "application/json", body, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("create = %d", resp.StatusCode)
	}
	if resp := post(t, srv.URL+"/v1/batches", "application/json", `{"batch_no":"","documents":[]}`, ""); resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("create invalid = %d", resp.StatusCode)
	}
	// Retry with the same content: 200. Same batch_no, different content: 409.
	if resp := post(t, srv.URL+"/v1/batches", "application/json", body, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("create again with same content = %d", resp.StatusCode)
	}
	conflict := `{"batch_no":"HTTP-2","documents":[{"document_no":"X","payload":{"a":999}}]}`
	if resp := post(t, srv.URL+"/v1/batches", "application/json", conflict, ""); resp.StatusCode != http.StatusConflict {
		t.Fatalf("create with different content = %d, want 409", resp.StatusCode)
	}

	resp, err := http.Get(srv.URL + "/v1/batches/HTTP-2")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st struct {
		State string         `json:"state"`
		Total int            `json:"total"`
		Count map[string]int `json:"counts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st.State != "PENDING" || st.Total != 2 || st.Count["PENDING"] != 2 {
		t.Fatalf("status = %+v", st)
	}

	notFound, _ := http.Get(srv.URL + "/v1/batches/NOPE")
	notFound.Body.Close()
	if notFound.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown batch = %d", notFound.StatusCode)
	}
	if resp := post(t, srv.URL+"/v1/batches/NOPE/process", "application/json", "", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("process unknown = %d", resp.StatusCode)
	}
	if resp := post(t, srv.URL+"/v1/batches/HTTP-2/process", "application/json", "", ""); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("process = %d", resp.StatusCode)
	}
}

// A batch body over the limit gets 413, like the callback endpoint, not a
// misleading "malformed JSON" 400. The handler is called directly so the
// test streams the oversized body without a network round trip.
func TestCreateBatch_BodyTooLarge(t *testing.T) {
	svc := docproc.NewService(memory.New(), acceptAll{})
	h := httpapi.New(svc, httpapi.NewRunner(context.Background(), nil), httpapi.Config{})

	body := io.MultiReader(
		strings.NewReader(`{"batch_no":"BIG","documents":[{"document_no":"A","payload":"`),
		io.LimitReader(repeatByte('x'), 33<<20),
		strings.NewReader(`"}]}`),
	)
	req := httptest.NewRequest(http.MethodPost, "/v1/batches", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (body: %s)", rec.Code, rec.Body.String())
	}
}

type repeatByte byte

func (b repeatByte) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(b)
	}
	return len(p), nil
}
