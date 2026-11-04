package lib

import (
	"net/http"
	"testing"
	"time"
)

func resp(code int, headers map[string]string) *http.Response {
	h := http.Header{}
	for k, v := range headers {
		h.Set(k, v)
	}
	return &http.Response{StatusCode: code, Header: h}
}

func fixedWindow(t0 time.Time) (*healthWindow, *time.Time) {
	now := t0
	return newHealthWindow(func() time.Time { return now }), &now
}

func TestHealthClassifiesResponses(t *testing.T) {
	w, _ := fixedWindow(time.Unix(1_800_000_000, 0))
	json := map[string]string{"content-type": "application/json"}
	w.observeResponse(resp(200, nil), 50*time.Millisecond)
	w.observeResponse(resp(404, json), 50*time.Millisecond)
	w.observeResponse(resp(429, map[string]string{"content-type": "application/json", "x-ratelimit-scope": "user"}), time.Second)
	w.observeResponse(resp(429, map[string]string{"content-type": "application/json", "x-ratelimit-scope": "shared"}), time.Second)
	w.observeResponse(resp(429, map[string]string{"content-type": "application/json", "x-ratelimit-scope": "global", "x-ratelimit-global": "true"}), time.Second)
	w.observeResponse(resp(429, map[string]string{"content-type": "text/html"}), time.Second)
	w.observeResponse(resp(502, nil), time.Second)
	w.observeTransportError()
	w.observeShed("deadline")

	h := w.Snapshot()
	if h.Requests != 8 || h.OK != 1 || h.ClientErrors != 1 || h.ServerErrors != 1 || h.TransportErr != 1 {
		t.Fatalf("unexpected counts: %+v", h)
	}
	if h.RateLimited["user"] != 1 || h.RateLimited["shared"] != 1 || h.RateLimited["global"] != 1 {
		t.Fatalf("rate limits: %+v", h.RateLimited)
	}
	if h.CloudflareBan != 1 || h.Shed["deadline"] != 1 {
		t.Fatalf("ban/shed: %+v", h)
	}
	if h.Status != "degraded" || h.Issues[0].Severity != "critical" {
		t.Fatalf("expected critical issue first: %+v", h.Issues)
	}
}

func TestHealthQuietWhenFine(t *testing.T) {
	w, _ := fixedWindow(time.Unix(1_800_000_000, 0))
	for i := 0; i < 100; i++ {
		w.observeResponse(resp(204, nil), 80*time.Millisecond)
	}
	h := w.Snapshot()
	if h.Status != "ok" || len(h.Issues) != 0 {
		t.Fatalf("expected ok, got %+v", h)
	}
	if h.P95Ms == nil || *h.P95Ms != 100 {
		t.Fatalf("p95: %v", h.P95Ms)
	}
}

func TestHealthFlagsDiscordErrors(t *testing.T) {
	w, _ := fixedWindow(time.Unix(1_800_000_000, 0))
	for i := 0; i < 90; i++ {
		w.observeResponse(resp(200, nil), time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		w.observeResponse(resp(503, nil), time.Millisecond)
	}
	h := w.Snapshot()
	if len(h.Issues) != 1 || h.Issues[0].Code != "discord_errors" || h.Issues[0].Severity != "critical" {
		t.Fatalf("issues: %+v", h.Issues)
	}
}

func TestHealthForgetsOldBuckets(t *testing.T) {
	w, now := fixedWindow(time.Unix(1_800_000_000, 0))
	w.observeResponse(resp(500, nil), time.Millisecond)
	*now = now.Add(healthWindowMinutes * time.Minute)
	w.observeResponse(resp(200, nil), time.Millisecond)
	h := w.Snapshot()
	if h.Requests != 1 || h.ServerErrors != 0 {
		t.Fatalf("old bucket kept: %+v", h)
	}
	if h.WindowSeconds != healthWindowMinutes*60 {
		t.Fatalf("window: %d", h.WindowSeconds)
	}
}
