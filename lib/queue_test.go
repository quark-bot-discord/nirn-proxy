package lib

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type fakeDiscord struct {
	calls  int64
	status int
	header http.Header
	delay  time.Duration
}

func (f *fakeDiscord) process(ctx context.Context, item *QueueItem) (*http.Response, error) {
	atomic.AddInt64(&f.calls, 1)
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	h := f.header
	if h == nil {
		h = http.Header{}
	}
	status := f.status
	if status == 0 {
		status = 200
	}
	for k, v := range h {
		(*item.Res).Header()[k] = v
	}
	(*item.Res).WriteHeader(status)
	return &http.Response{StatusCode: status, Header: h}, nil
}

func newTestQueue(f *fakeDiscord) *RequestQueue {
	return &RequestQueue{
		queues:         make(map[uint64]*QueueChannel),
		processor:      f.process,
		bufferSize:     10,
		isTokenInvalid: new(int64),
		identifier:     "test",
		queueType:      Bot,
	}
}

func doQueue(q *RequestQueue, req *http.Request, p Priority, deadline time.Time) (*httptest.ResponseRecorder, error) {
	rec := httptest.NewRecorder()
	var w http.ResponseWriter = rec
	err := q.Queue(req, &w, "/channels/!/messages", 42, p, deadline, time.Now())
	return rec, err
}

func TestQueueSendsWithinDeadline(t *testing.T) {
	f := &fakeDiscord{}
	q := newTestQueue(f)
	req := httptest.NewRequest("POST", "/api/v10/channels/1/messages", nil)
	rec, err := doQueue(q, req, PriorityHigh, time.Now().Add(time.Second))
	if err != nil || rec.Code != 200 || f.calls != 1 {
		t.Fatalf("err=%v code=%d calls=%d", err, rec.Code, f.calls)
	}
}

// A request that waited past its deadline behind a slow bucket is answered
// with a shed, and Discord is never called for it.
func TestQueueShedsExpiredItemWithoutSending(t *testing.T) {
	f := &fakeDiscord{delay: 80 * time.Millisecond}
	q := newTestQueue(f)

	first := make(chan struct{})
	go func() {
		req := httptest.NewRequest("POST", "/api/v10/channels/1/messages", nil)
		_, _ = doQueue(q, req, PriorityHigh, time.Time{})
		close(first)
	}()
	time.Sleep(10 * time.Millisecond)

	req := httptest.NewRequest("POST", "/api/v10/channels/1/messages", nil)
	rec, err := doQueue(q, req, PriorityHigh, time.Now().Add(20*time.Millisecond))
	<-first
	if err != nil {
		t.Fatalf("err %v", err)
	}
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get(HeaderShedReason) != ShedReasonQueueDeadline {
		t.Fatalf("want queue-deadline shed, got %d %v", rec.Code, rec.Header())
	}
	if rec.Header().Get(HeaderNotSent) != "true" {
		t.Fatal("shed response must say the request was not sent")
	}
	if c := atomic.LoadInt64(&f.calls); c != 1 {
		t.Fatalf("Discord called %d times, want 1 (the first request only)", c)
	}
}

// The client gave up while queued: nothing is sent and no token is taken.
func TestQueueSkipsCancelledClient(t *testing.T) {
	f := &fakeDiscord{delay: 60 * time.Millisecond}
	q := newTestQueue(f)
	var tokens int64
	q.takeGlobal = func(ctx context.Context, item *QueueItem) error {
		atomic.AddInt64(&tokens, 1)
		return nil
	}

	go func() {
		req := httptest.NewRequest("POST", "/api/v10/channels/1/messages", nil)
		_, _ = doQueue(q, req, PriorityHigh, time.Time{})
	}()
	time.Sleep(10 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest("POST", "/api/v10/channels/1/messages", nil).WithContext(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := doQueue(q, req, PriorityHigh, time.Time{})
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	if c := atomic.LoadInt64(&f.calls); c != 1 {
		t.Fatalf("Discord called %d times, want 1", c)
	}
	if n := atomic.LoadInt64(&tokens); n != 1 {
		t.Fatalf("global tokens taken %d, want 1", n)
	}
}

func TestQueueGlobalShedAnswersWithoutSending(t *testing.T) {
	f := &fakeDiscord{}
	q := newTestQueue(f)
	q.takeGlobal = func(ctx context.Context, item *QueueItem) error { return ErrShed }

	req := httptest.NewRequest("GET", "/api/v10/guilds/1/audit-logs", nil)
	rec, err := doQueue(q, req, PriorityLow, time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("err %v", err)
	}
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get(HeaderShedReason) != ShedReasonGlobalDeadline {
		t.Fatalf("want global-deadline shed, got %d %v", rec.Code, rec.Header())
	}
	if f.calls != 0 {
		t.Fatalf("Discord called %d times", f.calls)
	}
}

func TestQueueGlobal429LocksLimiter(t *testing.T) {
	h := http.Header{}
	h.Set("x-ratelimit-global", "true")
	h.Set("retry-after", "2")
	f := &fakeDiscord{status: 429, header: h}
	q := newTestQueue(f)
	locked := make(chan time.Time, 1)
	q.lockGlobal = func(until time.Time) { locked <- until }

	req := httptest.NewRequest("POST", "/api/v10/channels/1/messages", nil)
	if _, err := doQueue(q, req, PriorityHigh, time.Time{}); err != nil {
		t.Fatalf("err %v", err)
	}
	select {
	case until := <-locked:
		if d := time.Until(until); d < time.Second || d > 3*time.Second {
			t.Fatalf("lock for %v, want about 2s", d)
		}
	case <-time.After(time.Second):
		t.Fatal("global 429 did not lock the limiter")
	}
}

// End to end through the real limiter: with the global budget exhausted,
// high is served before low and low is shed at its deadline.
func TestQueueWithLimiterPrioritisesAndSheds(t *testing.T) {
	f := &fakeDiscord{}
	limiter := NewPriorityLimiter(1, 200*time.Millisecond)
	if err := limiter.Take(context.Background(), PriorityHigh, time.Time{}); err != nil {
		t.Fatal(err)
	}

	newQ := func() *RequestQueue {
		q := newTestQueue(f)
		q.takeGlobal = func(ctx context.Context, item *QueueItem) error {
			return limiter.Take(ctx, item.Priority, item.Deadline)
		}
		return q
	}
	// Separate queues, as different buckets would be.
	lowQ, highQ := newQ(), newQ()

	lowDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest("GET", "/api/v10/guilds/1/audit-logs", nil)
		rec, _ := doQueue(lowQ, req, PriorityLow, time.Now().Add(100*time.Millisecond))
		lowDone <- rec
	}()
	time.Sleep(10 * time.Millisecond)
	req := httptest.NewRequest("POST", "/api/v10/channels/1/messages", nil)
	highRec, _ := doQueue(highQ, req, PriorityHigh, time.Now().Add(time.Second))

	if highRec.Code != 200 {
		t.Fatalf("high: %d", highRec.Code)
	}
	if low := <-lowDone; low.Code != http.StatusServiceUnavailable {
		t.Fatalf("low should have been shed, got %d", low.Code)
	}
}
