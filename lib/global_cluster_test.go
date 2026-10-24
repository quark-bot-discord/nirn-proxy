package lib

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The cluster path: a node takes its global token from the node that owns the
// bot's limiter over HTTP. Shedding and cancellation must survive that hop.
func TestFireGlobalRequestAcrossNodes(t *testing.T) {
	ConfigureDiscordHTTPClient("", 5*time.Second, true, "", true)
	owner := NewQueueManager(10, 10)
	srv := httptest.NewServer(owner.CreateMux())
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	remote := NewClusterGlobalRateLimiter()
	const bot, limit = uint64(7), uint(1)

	if err := remote.FireGlobalRequest(context.Background(), addr, bot, limit, PriorityHigh, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("first token: %v", err)
	}
	// Window exhausted for a second; a low request with a short deadline is shed.
	err := remote.FireGlobalRequest(context.Background(), addr, bot, limit, PriorityLow, time.Now().Add(50*time.Millisecond))
	if !errors.Is(err, ErrShed) {
		t.Fatalf("want ErrShed across the hop, got %v", err)
	}

	// A cancelled caller is reported as cancelled, not as a failure.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = remote.FireGlobalRequest(ctx, addr, bot, limit, PriorityNormal, time.Time{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}

	// A lock forwarded to the owner pauses issuing there.
	if err := remote.FireGlobalLock(addr, bot, limit, time.Now().Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("lock: %v", err)
	}
	time.Sleep(1100 * time.Millisecond) // past the original window
	err = remote.FireGlobalRequest(context.Background(), addr, bot, limit, PriorityHigh, time.Now().Add(100*time.Millisecond))
	if !errors.Is(err, ErrShed) {
		t.Fatalf("token issued during a forwarded global lock: %v", err)
	}
}
