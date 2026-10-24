package lib

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// PriorityLimiter is the per-bot global rate limiter: `capacity` requests per
// `window`, fixed windows, matching what the leakybucket it replaces did.
//
// It differs from that bucket in three ways, each of which mattered on
// 24 Oct 2026 when main's proxy queued requests for up to 6m40s:
//
//  1. Take gives up when the caller's context ends or its deadline passes. The
//     old Take slept in a loop that never looked at the request, so a client
//     that had already timed out still consumed a token once its turn came -
//     and under overload, most of the tokens went to requests nobody was
//     waiting for any more.
//  2. Waiters are served strictly by priority, then in arrival order. A token
//     is never given to a lower class while a higher class is waiting.
//  3. A global 429 from Discord stops all issuing until it has expired
//     (LockUntil). The previous code recorded the lock and never read it.
type PriorityLimiter struct {
	mu          sync.Mutex
	capacity    uint
	window      time.Duration
	remaining   uint
	reset       time.Time
	lockedUntil time.Time
	waiters     [priorityCount]*list.List
	timer       *time.Timer
	now         func() time.Time
}

type limiterWaiter struct {
	ch      chan struct{}
	granted bool
	// grantedIn is the window the token came from, so a token handed back
	// after the window rolled over is not added to the next one.
	grantedIn time.Time
}

func NewPriorityLimiter(capacity uint, window time.Duration) *PriorityLimiter {
	l := &PriorityLimiter{
		capacity: capacity,
		window:   window,
		now:      time.Now,
	}
	for i := range l.waiters {
		l.waiters[i] = list.New()
	}
	l.remaining = capacity
	l.reset = l.now().Add(window)
	return l
}

// refill starts a new window if the current one has ended. Caller holds mu.
func (l *PriorityLimiter) refill(now time.Time) {
	if now.Before(l.lockedUntil) {
		l.remaining = 0
		return
	}
	if !now.Before(l.reset) {
		l.remaining = l.capacity
		l.reset = now.Add(l.window)
	}
}

// higherOrEqualWaiting reports whether anyone at priority p or above is queued.
// A newcomer must not overtake them. Caller holds mu.
func (l *PriorityLimiter) higherOrEqualWaiting(p Priority) bool {
	for i := Priority(0); i <= p; i++ {
		if l.waiters[i].Len() > 0 {
			return true
		}
	}
	return false
}

// nextIssueAt is when tokens can next be issued. Caller holds mu.
func (l *PriorityLimiter) nextIssueAt() time.Time {
	if l.lockedUntil.After(l.reset) {
		return l.lockedUntil
	}
	return l.reset
}

// grant hands out whatever tokens the current window has, highest priority
// first. Caller holds mu.
func (l *PriorityLimiter) grant(now time.Time) {
	l.refill(now)
	for p := Priority(0); p < priorityCount && l.remaining > 0; p++ {
		q := l.waiters[p]
		for q.Len() > 0 && l.remaining > 0 {
			w := q.Remove(q.Front()).(*limiterWaiter)
			w.granted = true
			w.grantedIn = l.reset
			l.remaining--
			close(w.ch)
		}
	}
	l.schedule(now)
}

// schedule arms the timer for the next window if anyone is still waiting.
// Caller holds mu.
func (l *PriorityLimiter) schedule(now time.Time) {
	waiting := false
	for _, q := range l.waiters {
		if q.Len() > 0 {
			waiting = true
			break
		}
	}
	if !waiting {
		if l.timer != nil {
			l.timer.Stop()
			l.timer = nil
		}
		return
	}
	delay := l.nextIssueAt().Sub(now)
	if delay < time.Millisecond {
		delay = time.Millisecond
	}
	if l.timer == nil {
		l.timer = time.AfterFunc(delay, l.onTimer)
	} else {
		l.timer.Reset(delay)
	}
}

func (l *PriorityLimiter) onTimer() {
	l.mu.Lock()
	defer l.mu.Unlock()
	// The timer is not cleared here: schedule() may already have re-armed it
	// for a later window while this call waited for mu, and clearing it would
	// orphan that one. grant() re-arms or stops it as needed.
	l.grant(l.now())
}

// Take blocks until a token is issued, the context ends, or the deadline
// passes (a zero deadline means none). Returns nil, ctx.Err(), or ErrShed. A
// caller that gets an error has not consumed a token.
func (l *PriorityLimiter) Take(ctx context.Context, p Priority, deadline time.Time) error {
	if p < 0 || p >= priorityCount {
		p = PriorityNormal
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	l.mu.Lock()
	now := l.now()
	if !deadline.IsZero() && !now.Before(deadline) {
		l.mu.Unlock()
		return ErrShed
	}
	l.refill(now)
	if l.remaining > 0 && !l.higherOrEqualWaiting(p) {
		l.remaining--
		l.mu.Unlock()
		return nil
	}
	w := &limiterWaiter{ch: make(chan struct{})}
	elem := l.waiters[p].PushBack(w)
	l.schedule(now)
	l.mu.Unlock()

	var deadlineC <-chan time.Time
	if !deadline.IsZero() {
		t := time.NewTimer(deadline.Sub(now))
		defer t.Stop()
		deadlineC = t.C
	}

	select {
	case <-w.ch:
		return nil
	case <-ctx.Done():
		return l.abandon(p, elem, w, ctx.Err())
	case <-deadlineC:
		return l.abandon(p, elem, w, ErrShed)
	}
}

// abandon removes a waiter that gave up. If it was granted in the same instant
// the token is returned to the window rather than lost.
func (l *PriorityLimiter) abandon(p Priority, elem *list.Element, w *limiterWaiter, err error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if w.granted {
		if w.grantedIn.Equal(l.reset) && l.remaining < l.capacity {
			l.remaining++
		}
		l.grant(l.now())
		return err
	}
	l.waiters[p].Remove(elem)
	l.schedule(l.now())
	return err
}

// LockUntil stops all issuing until t. Used when Discord answers with a global
// 429. A later lock extends an earlier one; an earlier one never shortens it.
func (l *PriorityLimiter) LockUntil(t time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if t.After(l.lockedUntil) {
		l.lockedUntil = t
	}
	now := l.now()
	l.refill(now)
	l.schedule(now)
}

// Waiting returns the number of queued callers per priority, for metrics.
func (l *PriorityLimiter) Waiting() [priorityCount]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out [priorityCount]int
	for i, q := range l.waiters {
		out[i] = q.Len()
	}
	return out
}
