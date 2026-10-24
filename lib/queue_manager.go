package lib

import (
	"context"
	"errors"
	lru "github.com/hashicorp/golang-lru"
	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type QueueType int64

const (
	Bot QueueType = iota
	NoAuth
	Bearer
)

// Some routes that have @me on the path don't really spread out through the cluster, causing issues
// and exacerbating tail latency hits from Discord. Only routes with no ratelimit headers should be put here
var pathsToRouteLocally = map[uint64]struct{}{
	HashCRC64("/users/@me/channels"): {},
	HashCRC64("/users/@me"):          {},
}

type QueueManager struct {
	sync.RWMutex
	queues                   map[string]*RequestQueue
	bearerQueues             *lru.Cache
	bearerMu                 sync.RWMutex
	bufferSize               int
	cluster                  *memberlist.Memberlist
	clusterGlobalRateLimiter *ClusterGlobalRateLimiter
	orderedClusterMembers    []string
	nameToAddressMap         map[string]string
	localNodeName            string
	localNodeIP              string
	localNodeProxyListenAddr string
	// defaultMaxWait applies to requests that do not send HeaderMaxWaitMs.
	// Zero means they wait indefinitely, as before.
	defaultMaxWait time.Duration
}

func onEvictLruItem(key interface{}, value interface{}) {
	go value.(*RequestQueue).destroy()
}

func NewQueueManager(bufferSize int, maxBearerLruSize int) *QueueManager {
	bearerMap, err := lru.NewWithEvict(maxBearerLruSize, onEvictLruItem)

	if err != nil {
		panic(err)
	}

	q := &QueueManager{
		queues:                   make(map[string]*RequestQueue),
		bearerQueues:             bearerMap,
		bufferSize:               bufferSize,
		cluster:                  nil,
		clusterGlobalRateLimiter: NewClusterGlobalRateLimiter(),
	}

	RegisterGlobalWaitingGauges(q.clusterGlobalRateLimiter.waiting)

	return q
}

// SetDefaultMaxWait sets the deadline for requests that do not carry
// HeaderMaxWaitMs. Call before serving.
func (m *QueueManager) SetDefaultMaxWait(d time.Duration) {
	m.defaultMaxWait = d
}

func botHashFor(q *RequestQueue) uint64 {
	if q.user != nil {
		return HashCRC64(q.user.Id)
	}
	return 0
}

// attachGlobal wires a queue to the global limiter. The token is now taken by
// the bucket's subscriber immediately before sending, not by the HTTP handler
// on arrival: a token taken on arrival was spent even when the request then
// waited minutes in its bucket and was abandoned, and the limiter's count no
// longer described what was actually reaching Discord.
func (m *QueueManager) attachGlobal(q *RequestQueue) {
	if q.identifier == "NoAuth" {
		return
	}
	q.takeGlobal = func(ctx context.Context, item *QueueItem) error {
		botHash := botHashFor(q)
		globalRouteTo := m.calculateRoute(botHash)
		if globalRouteTo == "" || q.queueType == Bearer {
			return m.clusterGlobalRateLimiter.Take(ctx, botHash, q.botLimit, item.Priority, item.Deadline)
		}
		return m.clusterGlobalRateLimiter.FireGlobalRequest(ctx, globalRouteTo, botHash, q.botLimit, item.Priority, item.Deadline)
	}
	q.lockGlobal = func(until time.Time) {
		botHash := botHashFor(q)
		globalRouteTo := m.calculateRoute(botHash)
		if globalRouteTo == "" || q.queueType == Bearer {
			m.clusterGlobalRateLimiter.LockUntil(botHash, q.botLimit, until)
			return
		}
		go func() {
			if err := m.clusterGlobalRateLimiter.FireGlobalLock(globalRouteTo, botHash, q.botLimit, until); err != nil {
				logger.WithField("function", "FireGlobalLock").Warn(err)
			}
		}()
	}
}

func (m *QueueManager) Shutdown() {
	if m.cluster != nil {
		m.cluster.Leave(30 * time.Second)
	}
}

func (m *QueueManager) reindexMembers() {
	if m.cluster == nil {
		logger.Warn("reindexMembers called but cluster is nil")
		return
	}

	m.Lock()
	defer m.Unlock()
	m.bearerMu.Lock()
	defer m.bearerMu.Unlock()

	members := m.cluster.Members()
	var orderedMembers []string
	nameToAddressMap := make(map[string]string)
	for _, m := range members {
		orderedMembers = append(orderedMembers, m.Name)
		nameToAddressMap[m.Name] = m.Addr.String() + ":" + string(m.Meta)
	}
	sort.Strings(orderedMembers)

	m.orderedClusterMembers = orderedMembers
	m.nameToAddressMap = nameToAddressMap
}

func (m *QueueManager) onNodeJoin(node *memberlist.Node) {
	// Running in goroutine prevents a deadlock inside memberlist
	go m.reindexMembers()
}
func (m *QueueManager) onNodeLeave(node *memberlist.Node) {
	// Running in goroutine prevents a deadlock inside memberlist
	go m.reindexMembers()
}

func (m *QueueManager) GetEventDelegate() *NirnEvents {
	return &NirnEvents{
		OnJoin:  m.onNodeJoin,
		OnLeave: m.onNodeLeave,
	}
}

func (m *QueueManager) SetCluster(cluster *memberlist.Memberlist, proxyPort string) {
	m.cluster = cluster
	m.localNodeName = cluster.LocalNode().Name
	m.localNodeIP = cluster.LocalNode().Addr.String()
	m.localNodeProxyListenAddr = m.localNodeIP + ":" + proxyPort
	m.reindexMembers()
}

func (m *QueueManager) calculateRoute(pathHash uint64) string {
	if m.cluster == nil {
		// Route to self, proxy in stand-alone mode
		return ""
	}

	if pathHash == 0 {
		return ""
	}

	if _, ok := pathsToRouteLocally[pathHash]; ok {
		return ""
	}

	m.RLock()
	defer m.RUnlock()
	m.bearerMu.RLock()
	defer m.bearerMu.RUnlock()

	members := m.orderedClusterMembers
	count := uint64(len(members))

	if count == 0 {
		return ""
	}

	chosenIndex := pathHash % count
	addr := m.nameToAddressMap[members[chosenIndex]]
	if addr == m.localNodeProxyListenAddr {
		return ""
	}
	return addr
}

func (m *QueueManager) routeRequest(addr string, req *http.Request) (*http.Response, error) {
	nodeReq, err := http.NewRequestWithContext(req.Context(), req.Method, "http://"+addr+req.URL.Path+"?"+req.URL.RawQuery, req.Body)
	nodeReq.Header = req.Header.Clone()
	nodeReq.Header.Set("nirn-routed-to", addr)
	if err != nil {
		return nil, err
	}

	logger.WithFields(logrus.Fields{
		"to":     addr,
		"path":   req.URL.Path,
		"method": req.Method,
	}).Trace("Routing request to node in cluster")
	resp, err := client.Do(nodeReq)
	logger.WithFields(logrus.Fields{
		"to":     addr,
		"path":   req.URL.Path,
		"method": req.Method,
	}).Trace("Received response from node")
	if err == nil {
		RequestsRoutedSent.Inc()
	} else {
		RequestsRoutedError.Inc()
	}

	return resp, err
}

func Generate429(resp *http.ResponseWriter) {
	writer := *resp
	writer.Header().Set("generated-by-proxy", "true")
	writer.Header().Set("x-ratelimit-scope", "user")
	writer.Header().Set("x-ratelimit-limit", "1")
	writer.Header().Set("x-ratelimit-remaining", "0")
	writer.Header().Set("x-ratelimit-reset", strconv.FormatInt(time.Now().Add(1*time.Second).Unix(), 10))
	writer.Header().Set("x-ratelimit-after", "1")
	writer.Header().Set("retry-after", "1")
	writer.Header().Set("content-type", "application/json")
	writer.WriteHeader(429)
	writer.Write([]byte("{\n\t\"global\": false,\n\t\"message\": \"You are being rate limited.\",\n\t\"retry_after\": 1\n}"))
}

func (m *QueueManager) getOrCreateBotQueue(token string) (*RequestQueue, error) {
	m.RLock()
	q, ok := m.queues[token]
	m.RUnlock()

	if !ok {
		m.Lock()
		defer m.Unlock()
		// Check if it wasn't created while we didn't hold the lock
		q, ok = m.queues[token]
		if !ok {
			var err error
			q, err = NewRequestQueue(ProcessRequest, token, m.bufferSize)

			if err != nil {
				return nil, err
			}

			m.attachGlobal(q)
			m.queues[token] = q
		}
	}

	return q, nil
}

func (m *QueueManager) getOrCreateBearerQueue(token string) (*RequestQueue, error) {
	m.bearerMu.RLock()
	q, ok := m.bearerQueues.Get(token)
	m.bearerMu.RUnlock()

	if !ok {
		m.bearerMu.Lock()
		defer m.bearerMu.Unlock()
		// Check if it wasn't created while we didn't hold the lock
		q, ok = m.bearerQueues.Get(token)
		if !ok {
			var err error
			var created *RequestQueue
			created, err = NewRequestQueue(ProcessRequest, token, 5)

			if err != nil {
				return nil, err
			}

			m.attachGlobal(created)
			m.bearerQueues.Add(token, created)
			q = created
		}
	}

	return q.(*RequestQueue), nil
}

func (m *QueueManager) DiscordRequestHandler(resp http.ResponseWriter, req *http.Request) {
	reqStart := time.Now()
	metricsPath := GetMetricsPath(req.URL.Path)
	ConnectionsOpen.With(map[string]string{"route": metricsPath, "method": req.Method}).Inc()
	defer ConnectionsOpen.With(map[string]string{"route": metricsPath, "method": req.Method}).Dec()

	token := req.Header.Get("Authorization")
	routingHash, path, queueType := m.GetRequestRoutingInfo(req, token)

	m.fulfillRequest(&resp, req, queueType, path, routingHash, token, reqStart)
}

func (m *QueueManager) GetRequestRoutingInfo(req *http.Request, token string) (routingHash uint64, path string, queueType QueueType) {
	path = GetOptimisticBucketPath(req.URL.Path, req.Method)
	queueType = NoAuth
	routingHash = HashCRC64(path)

	switch {
	case HasAuthPrefix(token, "Bearer"):
		queueType = Bearer
		routingHash = HashCRC64(token)
	case token != "" && !HasAuthPrefix(token, "Basic"):
		queueType = Bot
	}
	return
}

func (m *QueueManager) fulfillRequest(resp *http.ResponseWriter, req *http.Request, queueType QueueType, path string, pathHash uint64, token string, reqStart time.Time) {
	logEntry := logger.WithField("clientIp", req.RemoteAddr)
	forwdFor := req.Header.Get("X-Forwarded-For")
	if forwdFor != "" {
		logEntry = logEntry.WithField("forwardedFor", forwdFor)
	}
	routeTo := m.calculateRoute(pathHash)

	routeToHeader := req.Header.Get("nirn-routed-to")
	req.Header.Del("nirn-routed-to")

	if routeToHeader != "" {
		RequestsRoutedRecv.Inc()
	}

	priority := ParsePriority(req.Header.Get(HeaderPriority))
	deadline := RequestDeadline(req.Header, reqStart, m.defaultMaxWait)
	if !deadline.IsZero() {
		// Pinned as an absolute time so a request routed to another node does
		// not start a fresh budget there.
		req.Header.Set(HeaderDeadlineMs, strconv.FormatInt(deadline.UnixMilli(), 10))
	}

	var err error
	if routeTo == "" || routeToHeader != "" {
		var q *RequestQueue
		var err error
		if queueType == Bearer {
			q, err = m.getOrCreateBearerQueue(token)
		} else {
			q, err = m.getOrCreateBotQueue(token)
		}

		if err != nil {
			if strings.HasPrefix(err.Error(), "429") {
				Generate429(resp)
				logEntry.WithFields(logrus.Fields{"function": "getOrCreateQueue", "queueType": queueType}).Warn(err)
			} else {
				(*resp).WriteHeader(500)
				(*resp).Write([]byte(err.Error()))
				ErrorCounter.Inc()
				logEntry.WithFields(logrus.Fields{"function": "getOrCreateQueue", "queueType": queueType}).Error(err)
			}
			return
		}

		err = q.Queue(req, resp, path, pathHash, priority, deadline, reqStart)
		if err != nil {
			log := logEntry.WithField("function", "Queue")
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				log.WithField("waitedFor", time.Since(reqStart)).Warn(err)
			} else {
				log.Error(err)
			}
		}
	} else {
		var res *http.Response
		res, err = m.routeRequest(routeTo, req)
		if err == nil {
			err = CopyResponseToResponseWriter(res, resp)
			if err != nil {
				logEntry.WithField("function", "CopyResponseToResponseWriter").Error(err)
			}
		} else {
			logEntry = logEntry.WithField("function", "routeRequest")
			if !errors.Is(err, context.Canceled) {
				logEntry.Error(err)
			} else {
				logEntry.Warn(err)
			}
			// if it's a context canceled on the client it won't get the 429 anyway, if it's within the cluster we should retry
			Generate429(resp)
		}
	}
}

func (m *QueueManager) HandleGlobal(w http.ResponseWriter, r *http.Request) {
	botHashStr := r.Header.Get("bot-hash")
	botLimitStr := r.Header.Get("bot-limit")

	botHash, err := strconv.ParseUint(botHashStr, 10, 64)
	if err != nil {
		w.WriteHeader(400)
		return
	}

	botLimit, err := strconv.ParseUint(botLimitStr, 10, 64)
	if err != nil {
		w.WriteHeader(400)
		return
	}

	priority := ParsePriority(r.Header.Get(HeaderPriority))
	deadline := RequestDeadline(r.Header, time.Now(), 0)

	err = m.clusterGlobalRateLimiter.Take(r.Context(), botHash, uint(botLimit), priority, deadline)
	switch {
	case err == nil:
		w.WriteHeader(http.StatusOK)
		logger.Trace("Returned OK for global request")
	case errors.Is(err, ErrShed):
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		// The requesting node went away; nobody is reading the answer.
	}
}

func (m *QueueManager) HandleGlobalLock(w http.ResponseWriter, r *http.Request) {
	botHash, err := strconv.ParseUint(r.Header.Get("bot-hash"), 10, 64)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	botLimit, err := strconv.ParseUint(r.Header.Get("bot-limit"), 10, 64)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	untilMs, err := strconv.ParseInt(r.Header.Get("lock-until-ms"), 10, 64)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	m.clusterGlobalRateLimiter.LockUntil(botHash, uint(botLimit), time.UnixMilli(untilMs))
	w.WriteHeader(http.StatusOK)
}

func (m *QueueManager) CreateMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/", m.DiscordRequestHandler)
	mux.HandleFunc("/nirn/global", m.HandleGlobal)
	mux.HandleFunc("/nirn/global-lock", m.HandleGlobalLock)
	mux.HandleFunc("/nirn/healthz", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(200)
	})
	return mux
}
