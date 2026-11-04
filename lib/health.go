package lib

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// The proxy's last few minutes, for the status site.
//
// Prometheus already has all of this, but as cumulative counters that only
// SigNoz turns into rates. /healthz answers "what is Discord doing to us right
// now" directly: how many requests came back 5xx, whether we are being
// rate limited and by which scope, and whether Cloudflare has banned the
// egress IP outright (a 429 with no rate-limit headers, which is not a bucket
// and does not clear on its own).

const (
	healthWindowMinutes = 15
	healthBucket        = time.Minute
	minRequestsForRate  = 50
)

// Same edges, in seconds, as RequestHistogram.
var healthLatencyEdges = []float64{.1, .25, 1, 2.5, 5, 20}

type healthBucketData struct {
	start          time.Time
	ok             int // 2xx and 3xx
	clientErrors   int // 4xx other than 429: bad requests, 404s, 403s
	rateLimitUser  int
	rateLimitShare int
	globalLimited  int
	cloudflareBan  int
	serverErrors   int // 5xx
	transportError int // no response at all
	shed           map[string]int
	latency        []int
}

type healthWindow struct {
	mu      sync.Mutex
	buckets []*healthBucketData
	started time.Time
	now     func() time.Time
}

var proxyHealth = newHealthWindow(time.Now)

func newHealthWindow(now func() time.Time) *healthWindow {
	return &healthWindow{started: now(), now: now}
}

func (w *healthWindow) current() *healthBucketData {
	start := w.now().Truncate(healthBucket)
	if n := len(w.buckets); n > 0 && w.buckets[n-1].start.Equal(start) {
		return w.buckets[n-1]
	}
	b := &healthBucketData{start: start, shed: map[string]int{}, latency: make([]int, len(healthLatencyEdges)+1)}
	w.buckets = append(w.buckets, b)
	oldest := start.Add(-(healthWindowMinutes - 1) * healthBucket)
	for len(w.buckets) > 0 && w.buckets[0].start.Before(oldest) {
		w.buckets = w.buckets[1:]
	}
	return b
}

// isCloudflareBan: Discord's own 429s always carry x-ratelimit-scope and a JSON
// body. Cloudflare's 1015 ban has neither.
func isCloudflareBan(resp *http.Response) bool {
	return resp.StatusCode == 429 &&
		resp.Header.Get("x-ratelimit-scope") == "" &&
		!strings.Contains(resp.Header.Get("content-type"), "json")
}

// observeResponse records one answer from Discord. Never panics into the
// request path.
func (w *healthWindow) observeResponse(resp *http.Response, elapsed time.Duration) {
	defer func() { _ = recover() }()
	w.mu.Lock()
	defer w.mu.Unlock()
	b := w.current()
	code := resp.StatusCode
	switch {
	case isCloudflareBan(resp):
		b.cloudflareBan++
	case code == 429 && (resp.Header.Get("x-ratelimit-global") == "true" ||
		resp.Header.Get("x-ratelimit-scope") == "global"):
		b.globalLimited++
	case code == 429 && resp.Header.Get("x-ratelimit-scope") == "shared":
		b.rateLimitShare++
	case code == 429:
		b.rateLimitUser++
	case code >= 500:
		b.serverErrors++
	case code >= 400:
		b.clientErrors++
	default:
		b.ok++
	}
	secs := elapsed.Seconds()
	slot := len(healthLatencyEdges)
	for i, edge := range healthLatencyEdges {
		if secs <= edge {
			slot = i
			break
		}
	}
	b.latency[slot]++
}

func (w *healthWindow) observeTransportError() {
	defer func() { _ = recover() }()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.current().transportError++
}

func (w *healthWindow) observeShed(reason string) {
	defer func() { _ = recover() }()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.current().shed[reason]++
}

type HealthIssue struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

type ProxyHealth struct {
	Status        string         `json:"status"`
	WindowSeconds int            `json:"windowSeconds"`
	Requests      int            `json:"requests"`
	OK            int            `json:"ok"`
	ClientErrors  int            `json:"clientErrors"`
	RateLimited   map[string]int `json:"rateLimited"`
	CloudflareBan int            `json:"cloudflareBan"`
	ServerErrors  int            `json:"serverErrors"`
	TransportErr  int            `json:"transportErrors"`
	Shed          map[string]int `json:"shed"`
	// Share of requests Discord answered with 5xx or that got no answer.
	FailureRate *float64 `json:"failureRate"`
	// Upper edge of the bucket holding the percentile, in ms; null when idle.
	P95Ms  *int          `json:"p95Ms"`
	Issues []HealthIssue `json:"issues"`
}

func latencyPercentile(counts []int, q float64) *int {
	total := 0
	for _, c := range counts {
		total += c
	}
	if total == 0 {
		return nil
	}
	seen := 0
	for i, c := range counts {
		seen += c
		if float64(seen) >= q*float64(total) {
			idx := i
			if idx >= len(healthLatencyEdges) {
				idx = len(healthLatencyEdges) - 1
			}
			ms := int(healthLatencyEdges[idx] * 1000)
			return &ms
		}
	}
	return nil
}

func (w *healthWindow) Snapshot() ProxyHealth {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	oldest := now.Truncate(healthBucket).Add(-(healthWindowMinutes - 1) * healthBucket)
	h := ProxyHealth{
		RateLimited: map[string]int{"user": 0, "shared": 0, "global": 0},
		Shed:        map[string]int{},
		Issues:      []HealthIssue{},
	}
	latency := make([]int, len(healthLatencyEdges)+1)
	for _, b := range w.buckets {
		if b.start.Before(oldest) {
			continue
		}
		h.OK += b.ok
		h.ClientErrors += b.clientErrors
		h.RateLimited["user"] += b.rateLimitUser
		h.RateLimited["shared"] += b.rateLimitShare
		h.RateLimited["global"] += b.globalLimited
		h.CloudflareBan += b.cloudflareBan
		h.ServerErrors += b.serverErrors
		h.TransportErr += b.transportError
		for k, v := range b.shed {
			h.Shed[k] += v
		}
		for i := range latency {
			latency[i] += b.latency[i]
		}
	}
	covered := now.Sub(w.started)
	if covered > healthWindowMinutes*time.Minute {
		covered = healthWindowMinutes * time.Minute
	}
	h.WindowSeconds = int(covered.Seconds())
	h.Requests = h.OK + h.ClientErrors + h.RateLimited["user"] + h.RateLimited["shared"] +
		h.RateLimited["global"] + h.CloudflareBan + h.ServerErrors + h.TransportErr
	if h.Requests > 0 {
		r := float64(h.ServerErrors+h.TransportErr) / float64(h.Requests)
		r = float64(int(r*10000)) / 10000
		h.FailureRate = &r
	}
	h.P95Ms = latencyPercentile(latency, 0.95)
	h.Issues = deriveProxyIssues(h)
	h.Status = "ok"
	for _, i := range h.Issues {
		if i.Severity == "critical" {
			h.Status = "degraded"
			break
		}
		h.Status = "warning"
	}
	return h
}

func deriveProxyIssues(h ProxyHealth) []HealthIssue {
	issues := []HealthIssue{}
	if h.CloudflareBan > 0 {
		issues = append(issues, HealthIssue{"cloudflare_banned", "critical",
			fmt.Sprintf("Cloudflare refused %d requests: the egress IP is temporarily banned", h.CloudflareBan)})
	}
	if h.RateLimited["global"] > 0 {
		issues = append(issues, HealthIssue{"global_rate_limited", "critical",
			fmt.Sprintf("Discord's global rate limit was hit %d times", h.RateLimited["global"])})
	}
	if h.FailureRate != nil && h.Requests >= minRequestsForRate && *h.FailureRate > 0.01 {
		sev := "warning"
		if *h.FailureRate > 0.05 {
			sev = "critical"
		}
		issues = append(issues, HealthIssue{"discord_errors", sev,
			fmt.Sprintf("%.1f%% of requests to Discord failed (%d 5xx, %d no response)",
				*h.FailureRate*100, h.ServerErrors, h.TransportErr)})
	}
	limited := h.RateLimited["user"] + h.RateLimited["shared"]
	if h.Requests >= minRequestsForRate && float64(limited)/float64(h.Requests) > 0.05 {
		issues = append(issues, HealthIssue{"rate_limited", "warning",
			fmt.Sprintf("%d of %d requests were rate limited by Discord", limited, h.Requests)})
	}
	shed := 0
	for _, v := range h.Shed {
		shed += v
	}
	if shed > 0 && h.Requests+shed >= minRequestsForRate && float64(shed)/float64(h.Requests+shed) > 0.01 {
		issues = append(issues, HealthIssue{"proxy_shedding", "warning",
			fmt.Sprintf("%d requests were dropped by the proxy before reaching Discord", shed)})
	}
	sort.SliceStable(issues, func(a, b int) bool {
		return issues[a].Severity == "critical" && issues[b].Severity != "critical"
	})
	return issues
}

// HealthHandler is informational: always 200, whatever the window says. A
// slow Discord is not a reason to restart the proxy.
func HealthHandler(res http.ResponseWriter, _ *http.Request) {
	res.Header().Set("content-type", "application/json")
	res.Header().Set("cache-control", "no-store")
	_ = json.NewEncoder(res).Encode(proxyHealth.Snapshot())
}
