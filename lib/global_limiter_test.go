package lib

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func mustTake(t *testing.T, l *PriorityLimiter, p Priority) {
	t.Helper()
	if err := l.Take(context.Background(), p, time.Time{}); err != nil {
		t.Fatalf("take: %v", err)
	}
}

func TestLimiterIssuesCapacityPerWindow(t *testing.T) {
	l := NewPriorityLimiter(3, 200*time.Millisecond)
	for i := 0; i < 3; i++ {
		mustTake(t, l, PriorityNormal)
	}
	start := time.Now()
	mustTake(t, l, PriorityNormal)
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Fatalf("fourth take should wait for the next window, waited %v", waited)
	}
}

func TestLimiterServesHigherPriorityFirst(t *testing.T) {
	l := NewPriorityLimiter(1, 150*time.Millisecond)
	mustTake(t, l, PriorityNormal) // window exhausted

	var mu sync.Mutex
	var order []Priority
	var wg sync.WaitGroup
	start := func(p Priority) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mustTake(t, l, p)
			mu.Lock()
			order = append(order, p)
			mu.Unlock()
		}()
	}
	// Queue low first, then normal, then high: arrival order is the reverse
	// of the order they must be served in.
	start(PriorityLow)
	time.Sleep(10 * time.Millisecond)
	start(PriorityNormal)
	time.Sleep(10 * time.Millisecond)
	start(PriorityHigh)
	wg.Wait()

	want := []Priority{PriorityHigh, PriorityNormal, PriorityLow}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("served %v, want %v", order, want)
		}
	}
}

func TestLimiterNewcomerDoesNotOvertakeWaiters(t *testing.T) {
	l := NewPriorityLimiter(1, 100*time.Millisecond)
	mustTake(t, l, PriorityHigh)

	got := make(chan Priority, 2)
	go func() { mustTake(t, l, PriorityHigh); got <- PriorityHigh }()
	time.Sleep(10 * time.Millisecond)
	go func() { mustTake(t, l, PriorityLow); got <- PriorityLow }()

	if first := <-got; first != PriorityHigh {
		t.Fatalf("first served %v, want high", first)
	}
	<-got
}

func TestLimiterShedsAtDeadlineWithoutSpendingAToken(t *testing.T) {
	l := NewPriorityLimiter(1, time.Hour)
	mustTake(t, l, PriorityNormal)

	err := l.Take(context.Background(), PriorityLow, time.Now().Add(30*time.Millisecond))
	if !errors.Is(err, ErrShed) {
		t.Fatalf("want ErrShed, got %v", err)
	}
	if w := l.Waiting(); w[PriorityLow] != 0 {
		t.Fatalf("shed waiter still queued: %v", w)
	}
}

func TestLimiterExpiredDeadlineShedsImmediately(t *testing.T) {
	l := NewPriorityLimiter(10, time.Second)
	err := l.Take(context.Background(), PriorityHigh, time.Now().Add(-time.Millisecond))
	if !errors.Is(err, ErrShed) {
		t.Fatalf("want ErrShed, got %v", err)
	}
	// The token was not spent: all ten are still available.
	for i := 0; i < 10; i++ {
		if err := l.Take(context.Background(), PriorityNormal, time.Now().Add(10*time.Millisecond)); err != nil {
			t.Fatalf("token %d: %v", i, err)
		}
	}
}

// The incident this fork exists for: callers that had already gone away kept
// consuming tokens. A cancelled caller must leave the queue and spend nothing.
func TestLimiterCancelledCallerSpendsNothing(t *testing.T) {
	l := NewPriorityLimiter(1, 100*time.Millisecond)
	mustTake(t, l, PriorityNormal)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Take(ctx, PriorityHigh, time.Time{}) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	// The next window's single token must go to a live caller.
	start := time.Now()
	mustTake(t, l, PriorityLow)
	if time.Since(start) > 150*time.Millisecond {
		t.Fatalf("live caller waited past the next window")
	}
}

func TestLimiterAlreadyCancelledContext(t *testing.T) {
	l := NewPriorityLimiter(5, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Take(ctx, PriorityNormal, time.Time{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestLimiterLockUntilPausesIssuing(t *testing.T) {
	l := NewPriorityLimiter(100, 20*time.Millisecond)
	l.LockUntil(time.Now().Add(120 * time.Millisecond))

	start := time.Now()
	mustTake(t, l, PriorityHigh)
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Fatalf("take during a global lock should wait for it, waited %v", waited)
	}
}

func TestLimiterLockIsNeverShortened(t *testing.T) {
	l := NewPriorityLimiter(100, 20*time.Millisecond)
	l.LockUntil(time.Now().Add(120 * time.Millisecond))
	l.LockUntil(time.Now().Add(10 * time.Millisecond))

	start := time.Now()
	mustTake(t, l, PriorityHigh)
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Fatalf("a shorter lock shortened the longer one: waited %v", waited)
	}
}

func TestLimiterThroughputUnderContention(t *testing.T) {
	// 50 callers, 10 per 50ms window: all served in about 5 windows, none lost.
	l := NewPriorityLimiter(10, 50*time.Millisecond)
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- l.Take(context.Background(), Priority(i%3), time.Now().Add(2*time.Second))
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("unexpected error under contention: %v", err)
		}
	}
	if w := l.Waiting(); w != [priorityCount]int{} {
		t.Fatalf("waiters left behind: %v", w)
	}
}
