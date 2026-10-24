package lib

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Request classes, highest first. A client names its class on
// HeaderPriority; anything missing or unrecognised is PriorityNormal, so a
// client that knows nothing about this behaves exactly as before.
//
// The global limiter serves waiting requests strictly in this order. Under a
// sustained overload that means low-priority requests wait until their
// deadline and are shed, which is the point: a lookup that only adds detail to
// a log must not delay the log itself.
type Priority int

const (
	PriorityHigh Priority = iota
	PriorityNormal
	PriorityLow
	priorityCount
)

const (
	// HeaderPriority is "high", "normal" or "low".
	HeaderPriority = "x-quark-priority"
	// HeaderMaxWaitMs is how long, in milliseconds from arrival at the first
	// proxy node, the request may wait for a rate limit before it is shed.
	HeaderMaxWaitMs = "x-quark-max-wait-ms"
	// HeaderDeadlineMs is the absolute deadline (unix ms) derived from
	// HeaderMaxWaitMs on the first node. Set when routing inside the cluster so
	// a forwarded request does not get a fresh budget on the second node.
	HeaderDeadlineMs = "x-quark-deadline-ms"
	// HeaderShedReason is set on responses the proxy generated itself without
	// contacting Discord.
	HeaderShedReason = "x-quark-shed-reason"
	// HeaderNotSent is "true" on every response where the request is known not
	// to have reached Discord, so the client can retry without any risk of a
	// duplicate.
	HeaderNotSent = "x-quark-not-sent"
)

const (
	ShedReasonQueueDeadline  = "proxy_queue_deadline"
	ShedReasonGlobalDeadline = "proxy_global_deadline"
)

// ErrShed means the request was dropped before it was sent to Discord.
var ErrShed = errors.New("request shed by proxy before sending")

func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityLow:
		return "low"
	default:
		return "normal"
	}
}

func ParsePriority(raw string) Priority {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "high":
		return PriorityHigh
	case "low":
		return PriorityLow
	default:
		return PriorityNormal
	}
}

// RequestDeadline resolves the deadline for a request arriving now.
//
// An absolute deadline from another node wins. Otherwise the client's
// HeaderMaxWaitMs, otherwise defaultMaxWait. A zero result means no deadline.
func RequestDeadline(header http.Header, now time.Time, defaultMaxWait time.Duration) time.Time {
	if raw := header.Get(HeaderDeadlineMs); raw != "" {
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil && ms > 0 {
			return time.UnixMilli(ms)
		}
	}
	if raw := header.Get(HeaderMaxWaitMs); raw != "" {
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil && ms > 0 {
			return now.Add(time.Duration(ms) * time.Millisecond)
		}
	}
	if defaultMaxWait > 0 {
		return now.Add(defaultMaxWait)
	}
	return time.Time{}
}

// StripQuarkHeaders removes the proxy's own request headers before the request
// is forwarded to Discord.
func StripQuarkHeaders(header http.Header) {
	for key := range header {
		if strings.HasPrefix(strings.ToLower(key), "x-quark-") {
			header.Del(key)
		}
	}
}

// WriteShed answers a request the proxy dropped before sending it. 503 rather
// than 429: this is not Discord's rate limit, and a client that treats every
// 429 as "wait retry_after and resend" should not do that blindly here.
func WriteShed(res http.ResponseWriter, reason string) {
	h := res.Header()
	h.Set("generated-by-proxy", "true")
	h.Set(HeaderShedReason, reason)
	h.Set(HeaderNotSent, "true")
	h.Set("retry-after", "1")
	h.Set("content-type", "application/json")
	res.WriteHeader(http.StatusServiceUnavailable)
	_, _ = res.Write([]byte(`{"message":"Request shed by nirn-proxy before it was sent to Discord","code":0,"shed_reason":"` + reason + `"}`))
}
