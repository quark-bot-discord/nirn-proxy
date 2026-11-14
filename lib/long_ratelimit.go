package lib

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Long rate limits: answer at once instead of queueing.
//
// After a 429 a bucket's goroutine sleeps until the limit resets, and every
// request that arrived meanwhile used to sit behind that sleep until its
// client gave up. On 14 Nov 2026 Discord limited GET /guilds/:id/vanity-url
// for ~58 minutes at a time, and every member join on main waited 15s per
// attempt, three attempts, for a request that could not be sent before the
// next hour. Now a request that cannot be sent within its own wait budget is
// answered immediately with a 429 carrying the real retry_after. It is never
// sent, so it costs Discord nothing and the client learns the truth at once.
//
// Some long limits belong to a route, not a guild: Discord sends no
// x-ratelimit-bucket and every guild's 429 resets at the same instant (all of
// the vanity-url 429s on 14 Nov reset at 22:12:24). The proxy keys buckets per
// guild, so each guild would spend one real 429 finding that out, and real
// 429s count toward Cloudflare's 10,000 invalid requests per 10 minutes - the
// ban that takes the whole bot off the API. A long 429 without a Discord
// bucket therefore locks the route, ids replaced by "!", for every guild
// until it resets.

// ShedReasonBucketRatelimited and ShedReasonRouteRatelimited name why a
// request was answered without being sent.
const (
	ShedReasonBucketRatelimited = "bucket_ratelimited"
	ShedReasonRouteRatelimited  = "route_ratelimited"
)

var (
	// failFastWithoutDeadline is the wait budget assumed for a request that
	// sends no deadline. Past it the client has almost certainly gone.
	failFastWithoutDeadline = 10 * time.Second
	// routeLockMinReset is the shortest unbucketed 429 that locks the whole
	// route. Zero disables route locks.
	routeLockMinReset = 10 * time.Minute
)

// ConfigureLongRatelimits sets the two thresholds above.
func ConfigureLongRatelimits(failFast time.Duration, routeLockMin time.Duration) {
	if failFast > 0 {
		failFastWithoutDeadline = failFast
	}
	routeLockMinReset = routeLockMin
}

// RouteKey is a bucket path with every snowflake replaced by "!", so
// /guilds/1/vanity-url and /guilds/2/vanity-url share one key.
func RouteKey(path string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if IsSnowflake(part) {
			parts[i] = "!"
		}
	}
	return strings.Join(parts, "/")
}

// waitBudget is how long a request may still wait for its rate limit.
func waitBudget(deadline time.Time, now time.Time) time.Duration {
	if deadline.IsZero() {
		return failFastWithoutDeadline
	}
	return deadline.Sub(now)
}

// blockedFor is how long a request on this bucket must wait before it could be
// sent, and why. Zero when nothing is blocking it.
func (q *RequestQueue) blockedFor(ch *QueueChannel, path string, now time.Time) (time.Duration, string) {
	wait := time.Duration(ch.blockedUntil.Load() - now.UnixNano())
	reason := ShedReasonBucketRatelimited
	key := RouteKey(path)
	if v, ok := q.routeLocks.Load(key); ok {
		routeWait := time.Duration(v.(int64) - now.UnixNano())
		if routeWait <= 0 {
			q.routeLocks.CompareAndDelete(key, v)
		} else if routeWait > wait {
			wait, reason = routeWait, ShedReasonRouteRatelimited
		}
	}
	if wait < 0 {
		wait = 0
	}
	return wait, reason
}

// cannotWait reports whether the item would outlive its budget waiting
// `wait`, and so should be answered now.
func cannotWait(item *QueueItem, wait time.Duration, now time.Time) bool {
	return wait > 0 && wait > waitBudget(item.Deadline, now)
}

// shouldLockRoute reports whether a 429 is a route-wide limit by the rule
// above: long, unbucketed, and neither global nor shared.
func shouldLockRoute(resp *http.Response, scope string, isGlobal bool, resetAfter time.Duration) bool {
	return routeLockMinReset > 0 &&
		resp.StatusCode == http.StatusTooManyRequests &&
		!isGlobal &&
		scope != "shared" &&
		resp.Header.Get("x-ratelimit-bucket") == "" &&
		resetAfter >= routeLockMinReset
}

// lockRoute blocks every guild's bucket on this route until `until`.
func (q *RequestQueue) lockRoute(path string, until time.Time) {
	key := RouteKey(path)
	if v, loaded := q.routeLocks.LoadOrStore(key, until.UnixNano()); loaded {
		if v.(int64) >= until.UnixNano() {
			return
		}
		q.routeLocks.Store(key, until.UnixNano())
	}
	RouteLockCounter.With(map[string]string{"route": key}).Inc()
	logger.WithField("route", key).WithField("until", until).Warn("Route-wide rate limit, locking route for every guild")
}

// shedRatelimited answers a request that cannot be sent before its budget
// runs out, with the real wait. Never sent, so a retry duplicates nothing.
func shedRatelimited(item *QueueItem, reason string, wait time.Duration) {
	observeDropped(item, "shed", reason)
	WriteRatelimitShed(*item.Res, reason, wait)
}

// WriteRatelimitShed writes a 429 the client can treat like Discord's: it
// carries retry_after. x-quark-not-sent says it never reached Discord.
func WriteRatelimitShed(res http.ResponseWriter, reason string, wait time.Duration) {
	seconds := math.Ceil(wait.Seconds()*1000) / 1000
	h := res.Header()
	h.Set("generated-by-proxy", "true")
	h.Set(HeaderShedReason, reason)
	h.Set(HeaderNotSent, "true")
	h.Set("retry-after", strconv.FormatInt(int64(math.Ceil(wait.Seconds())), 10))
	h.Set("content-type", "application/json")
	res.WriteHeader(http.StatusTooManyRequests)
	_, _ = res.Write([]byte(`{"message":"Rate limited; not sent to Discord by nirn-proxy","retry_after":` +
		strconv.FormatFloat(seconds, 'f', -1, 64) + `,"global":false,"code":0,"shed_reason":"` + reason + `"}`))
}

// waitOutReset replaces the plain sleep until a bucket's limit resets. While
// it waits it keeps taking requests off the channel: those that cannot wait
// that long are answered at once, and the rest are returned, in arrival
// order, to be sent first once the limit has reset. closed reports that the
// channel was closed (swept) during the wait.
func (q *RequestQueue) waitOutReset(ch *QueueChannel, path string, until time.Time) (held []*QueueItem, closed bool) {
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return held, closed
		case item, ok := <-ch.ch:
			if !ok {
				// Nothing more can arrive; still honour the reset for what is held.
				closed = true
				<-timer.C
				return held, closed
			}
			now := time.Now()
			if wait, reason := q.blockedFor(ch, path, now); cannotWait(item, wait, now) {
				shedRatelimited(item, reason, wait)
				item.doneChan <- nil
				continue
			}
			held = append(held, item)
		}
	}
}
