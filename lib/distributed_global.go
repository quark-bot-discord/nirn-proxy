package lib

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type ClusterGlobalRateLimiter struct {
	sync.RWMutex
	limiters map[uint64]*PriorityLimiter
}

func NewClusterGlobalRateLimiter() *ClusterGlobalRateLimiter {
	return &ClusterGlobalRateLimiter{
		limiters: make(map[uint64]*PriorityLimiter),
	}
}

// Take waits for a global token for this bot. See PriorityLimiter.Take: it
// returns ctx.Err() if the caller went away and ErrShed if the deadline passed,
// and in both cases no token was consumed.
func (c *ClusterGlobalRateLimiter) Take(ctx context.Context, botHash uint64, botLimit uint, p Priority, deadline time.Time) error {
	start := time.Now()
	err := c.getOrCreate(botHash, botLimit).Take(ctx, p, deadline)
	outcome := "granted"
	switch {
	case errors.Is(err, ErrShed):
		outcome = "shed"
	case err != nil:
		outcome = "cancelled"
	}
	GlobalWaitHistogram.With(map[string]string{"priority": p.String(), "outcome": outcome}).Observe(time.Since(start).Seconds())
	return err
}

// LockUntil pauses the bot's global limiter, after a global 429 from Discord.
func (c *ClusterGlobalRateLimiter) LockUntil(botHash uint64, botLimit uint, until time.Time) {
	c.getOrCreate(botHash, botLimit).LockUntil(until)
}

func (c *ClusterGlobalRateLimiter) getOrCreate(botHash uint64, botLimit uint) *PriorityLimiter {
	c.RLock()
	l, ok := c.limiters[botHash]
	c.RUnlock()
	if ok {
		return l
	}
	c.Lock()
	defer c.Unlock()
	// Check if it wasn't created while we didn't hold the exclusive lock
	if l, ok = c.limiters[botHash]; ok {
		return l
	}
	l = NewPriorityLimiter(botLimit, 1*time.Second)
	c.limiters[botHash] = l
	return l
}

// waiting sums queued callers per priority across bots, for the gauge.
func (c *ClusterGlobalRateLimiter) waiting() [priorityCount]int {
	c.RLock()
	defer c.RUnlock()
	var out [priorityCount]int
	for _, l := range c.limiters {
		w := l.Waiting()
		for i := range out {
			out[i] += w[i]
		}
	}
	return out
}

func setGlobalHeaders(req *http.Request, botHash uint64, botLimit uint) {
	req.Header.Set("bot-hash", strconv.FormatUint(botHash, 10))
	req.Header.Set("bot-limit", strconv.FormatUint(uint64(botLimit), 10))
}

// FireGlobalRequest takes a global token from the node that owns this bot's
// limiter. The remote Take honours the same context, priority and deadline.
func (c *ClusterGlobalRateLimiter) FireGlobalRequest(ctx context.Context, addr string, botHash uint64, botLimit uint, p Priority, deadline time.Time) error {
	globalReq, err := http.NewRequestWithContext(ctx, "GET", "http://"+addr+"/nirn/global", nil)
	if err != nil {
		return err
	}

	setGlobalHeaders(globalReq, botHash, botLimit)
	globalReq.Header.Set(HeaderPriority, p.String())
	if !deadline.IsZero() {
		globalReq.Header.Set(HeaderDeadlineMs, strconv.FormatInt(deadline.UnixMilli(), 10))
	}

	// The node handling the request will only return if we grabbed a token or an error was thrown
	resp, err := client.Do(globalReq)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	defer resp.Body.Close()
	logger.Trace("Got go-ahead for global")

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusServiceUnavailable:
		return ErrShed
	default:
		return errors.New("global request failed with status " + resp.Status)
	}
}

// FireGlobalLock forwards a global 429 to the node that owns the limiter.
func (c *ClusterGlobalRateLimiter) FireGlobalLock(addr string, botHash uint64, botLimit uint, until time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", "http://"+addr+"/nirn/global-lock", nil)
	if err != nil {
		return err
	}
	setGlobalHeaders(req, botHash, botLimit)
	req.Header.Set("lock-until-ms", strconv.FormatInt(until.UnixMilli(), 10))
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("global lock failed with status " + resp.Status)
	}
	return nil
}
