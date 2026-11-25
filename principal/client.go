// Package principal implements docproc.PrincipalClient over HTTP.
package principal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/sabiqsa/go-document-processor/docproc"
)

type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient creates a client. The per-request timeout comes from the ctx set
// by the Service (WithSubmitTimeout), so http.Client needs no Timeout of its own.
func NewClient(baseURL string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{}
	}
	return &Client{baseURL: baseURL, http: hc}
}

type submitRequest struct {
	DocumentID string          `json:"document_id"`
	DocumentNo string          `json:"document_no"`
	Payload    json.RawMessage `json:"payload"`
}

func (c *Client) Submit(ctx context.Context, doc docproc.Document) error {
	body, err := json.Marshal(submitRequest{DocumentID: doc.ID, DocumentNo: doc.DocumentNo, Payload: doc.Payload})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/documents", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	// Per-attempt idempotency key. If the same request is resent by a
	// network-level retry, the Principal can recognise it. See section C of
	// docs/answers.md for per-attempt vs per-document keys.
	req.Header.Set("Idempotency-Key", fmt.Sprintf("doc-%s-attempt-%d", doc.ID, doc.AttemptCount))

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("principal request: %w", err)
	}
	defer resp.Body.Close()
	// Drain the body so the connection can be reused (keep-alive).
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("principal responded %d: %s", resp.StatusCode, bytes.TrimSpace(snippet))
	}
	return nil
}
