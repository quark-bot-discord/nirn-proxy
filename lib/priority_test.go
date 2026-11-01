package lib

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestParsePriority(t *testing.T) {
	cases := map[string]Priority{
		"high": PriorityHigh, "HIGH": PriorityHigh, " low ": PriorityLow,
		"normal": PriorityNormal, "": PriorityNormal, "urgent": PriorityNormal,
	}
	for raw, want := range cases {
		if got := ParsePriority(raw); got != want {
			t.Errorf("ParsePriority(%q) = %v, want %v", raw, got, want)
		}
	}
}

func TestRequestDeadline(t *testing.T) {
	now := time.UnixMilli(1_000_000)

	h := http.Header{}
	if d := RequestDeadline(h, now, 0); !d.IsZero() {
		t.Fatalf("no header, no default: want zero, got %v", d)
	}
	if d := RequestDeadline(h, now, 25*time.Second); !d.Equal(now.Add(25 * time.Second)) {
		t.Fatalf("default not applied: %v", d)
	}

	h.Set(HeaderMaxWaitMs, "5000")
	if d := RequestDeadline(h, now, 25*time.Second); !d.Equal(now.Add(5 * time.Second)) {
		t.Fatalf("client max wait not applied: %v", d)
	}

	// An absolute deadline from the first node wins over a relative budget,
	// so routing does not reset the clock.
	h.Set(HeaderDeadlineMs, strconv.FormatInt(now.Add(time.Second).UnixMilli(), 10))
	if d := RequestDeadline(h, now.Add(500*time.Millisecond), 25*time.Second); !d.Equal(now.Add(time.Second)) {
		t.Fatalf("absolute deadline not preferred: %v", d)
	}

	bad := http.Header{}
	bad.Set(HeaderMaxWaitMs, "soon")
	if d := RequestDeadline(bad, now, 0); !d.IsZero() {
		t.Fatalf("garbage max wait should be ignored: %v", d)
	}
}

func TestStripQuarkHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bot x")
	h.Set(HeaderPriority, "low")
	h.Set(HeaderDeadlineMs, "1")
	h.Set("X-Quark-Anything", "y")
	StripQuarkHeaders(h)
	if h.Get("Authorization") == "" {
		t.Fatal("stripped a non-quark header")
	}
	for k := range h {
		if len(k) >= 8 && (k[:8] == "X-Quark-" || k[:8] == "x-quark-") {
			t.Fatalf("left %s", k)
		}
	}
}

func TestWriteShed(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteShed(rec, ShedReasonQueueDeadline)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d", rec.Code)
	}
	if rec.Header().Get(HeaderNotSent) != "true" || rec.Header().Get(HeaderShedReason) != ShedReasonQueueDeadline {
		t.Fatalf("headers %v", rec.Header())
	}
}
