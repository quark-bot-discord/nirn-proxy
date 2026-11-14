package lib

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func doQueuePath(q *RequestQueue, path string, deadline time.Time) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	var w http.ResponseWriter = rec
	req := httptest.NewRequest("GET", "/api/v10"+path, nil)
	_ = q.Queue(req, &w, path, HashCRC64(path), PriorityNormal, deadline, time.Now())
	return rec
}

func limitedFor(seconds float64, bucket string) http.Header {
	h := http.Header{}
	s := strconv.FormatFloat(seconds, 'f', -1, 64)
	h.Set("retry-after", s)
	h.Set("x-ratelimit-reset-after", s)
	h.Set("x-ratelimit-limit", "1")
	h.Set("x-ratelimit-remaining", "0")
	if bucket != "" {
		h.Set("x-ratelimit-bucket", bucket)
	}
	return h
}

func TestRouteKey(t *testing.T) {
	got := RouteKey("/guilds/427481605225840640/vanity-url")
	if got != "/guilds/!/vanity-url" {
		t.Fatalf("got %q", got)
	}
	if RouteKey("/channels/!/messages") != "/channels/!/messages" {
		t.Fatal("an already-generic path must be unchanged")
	}
}

// After a 429 that resets an hour out, the next request on the bucket is
// answered at once with the real wait, and Discord is not called again.
func TestLongBucketLimitAnswersImmediately(t *testing.T) {
	f := &fakeDiscord{status: 429, header: limitedFor(3600, "abc")}
	q := newTestQueue(f)
	path := "/guilds/427481605225840640/vanity-url"

	doQueuePath(q, path, time.Now().Add(15*time.Second))

	start := time.Now()
	rec := doQueuePath(q, path, time.Now().Add(15*time.Second))
	if took := time.Since(start); took > time.Second {
		t.Fatalf("waited %v; should be answered at once", took)
	}
	if rec.Code != 429 || rec.Header().Get(HeaderShedReason) != ShedReasonBucketRatelimited {
		t.Fatalf("want bucket_ratelimited 429, got %d %v", rec.Code, rec.Header())
	}
	if rec.Header().Get(HeaderNotSent) != "true" {
		t.Fatal("must say it was not sent")
	}
	if ra, _ := strconv.Atoi(rec.Header().Get("retry-after")); ra < 3500 {
		t.Fatalf("retry-after %d, want the real ~3600", ra)
	}
	if c := atomic.LoadInt64(&f.calls); c != 1 {
		t.Fatalf("Discord called %d times, want 1", c)
	}
}

// A long 429 with no Discord bucket locks the route for every guild: a
// different guild is answered at once without its own 429 from Discord.
func TestRouteWideLimitCoversOtherGuilds(t *testing.T) {
	f := &fakeDiscord{status: 429, header: limitedFor(3600, "")}
	q := newTestQueue(f)

	doQueuePath(q, "/guilds/427481605225840640/vanity-url", time.Now().Add(15*time.Second))

	rec := doQueuePath(q, "/guilds/1445873467290288252/vanity-url", time.Now().Add(15*time.Second))
	if rec.Code != 429 || rec.Header().Get(HeaderShedReason) != ShedReasonRouteRatelimited {
		t.Fatalf("want route_ratelimited 429, got %d %v", rec.Code, rec.Header())
	}
	if c := atomic.LoadInt64(&f.calls); c != 1 {
		t.Fatalf("Discord called %d times, want 1", c)
	}

	// Other routes in the same guild are untouched.
	f.status, f.header = 200, http.Header{}
	if rec := doQueuePath(q, "/guilds/1445873467290288252/invites", time.Now().Add(15*time.Second)); rec.Code != 200 {
		t.Fatalf("unrelated route got %d", rec.Code)
	}
}

// A long limit that has a Discord bucket is per guild, not route-wide.
func TestBucketedLimitDoesNotLockRoute(t *testing.T) {
	f := &fakeDiscord{status: 429, header: limitedFor(3600, "abc")}
	q := newTestQueue(f)

	doQueuePath(q, "/guilds/427481605225840640/vanity-url", time.Now().Add(15*time.Second))
	f.status, f.header = 200, http.Header{}
	if rec := doQueuePath(q, "/guilds/1445873467290288252/vanity-url", time.Now().Add(15*time.Second)); rec.Code != 200 {
		t.Fatalf("other guild got %d, want it sent", rec.Code)
	}
}

// A short reset is still waited out and the request is sent, as before.
func TestShortLimitStillQueues(t *testing.T) {
	f := &fakeDiscord{status: 429, header: limitedFor(0.2, "abc")}
	q := newTestQueue(f)
	path := "/guilds/427481605225840640/vanity-url"

	doQueuePath(q, path, time.Now().Add(15*time.Second))
	f.status, f.header = 200, http.Header{}

	start := time.Now()
	rec := doQueuePath(q, path, time.Now().Add(15*time.Second))
	if rec.Code != 200 {
		t.Fatalf("got %d, want sent after the reset", rec.Code)
	}
	if took := time.Since(start); took < 100*time.Millisecond {
		t.Fatalf("sent after %v, before the limit reset", took)
	}
	if c := atomic.LoadInt64(&f.calls); c != 2 {
		t.Fatalf("Discord called %d times, want 2", c)
	}
}
