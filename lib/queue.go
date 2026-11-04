package lib

import (
	"context"
	"errors"
	"github.com/Clever/leakybucket"
	"github.com/Clever/leakybucket/memory"
	"github.com/sirupsen/logrus"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type QueueItem struct {
	Req      *http.Request
	Res      *http.ResponseWriter
	doneChan chan *http.Response
	errChan  chan error
	// Priority decides the order in which waiting requests get global tokens.
	Priority Priority
	// Deadline is when the request stops being worth sending. Zero means none.
	Deadline time.Time
	// Arrived is when the proxy first saw the request, for metrics.
	Arrived time.Time
}

type QueueChannel struct {
	ch       chan *QueueItem
	lastUsed time.Time
}

type RequestQueue struct {
	sync.RWMutex
	// bucket path hash as key
	queues       map[uint64]*QueueChannel
	processor    func(ctx context.Context, item *QueueItem) (*http.Response, error)
	globalBucket leakybucket.Bucket
	// bufferSize Defines the size of the request channel buffer for each bucket
	bufferSize     int
	user           *BotUserResponse
	identifier     string
	isTokenInvalid *int64
	botLimit       uint
	queueType      QueueType
	// takeGlobal waits for a global token immediately before the request is
	// sent. Nil means no global limit (NoAuth). Set by the QueueManager.
	takeGlobal func(ctx context.Context, item *QueueItem) error
	// lockGlobal pauses the global limiter after a global 429. May be nil.
	lockGlobal func(until time.Time)
}

func NewRequestQueue(processor func(ctx context.Context, item *QueueItem) (*http.Response, error), token string, bufferSize int) (*RequestQueue, error) {
	queueType := NoAuth
	var user *BotUserResponse
	var err error
	switch {
	case HasAuthPrefix(token, "Bearer"):
		queueType = Bearer
	case token != "" && !HasAuthPrefix(token, "Basic"):
		user, err = GetBotUser(token)
		if err != nil {
			return nil, err
		}
	}

	limit, err := GetBotGlobalLimit(token, user)
	memStorage := memory.New()
	globalBucket, _ := memStorage.Create("global", limit, 1*time.Second)
	if err != nil {
		if strings.HasPrefix(err.Error(), "invalid token") {
			// Return a queue that will only return 401s
			var invalid = new(int64)
			*invalid = 999
			return &RequestQueue{
				queues:         make(map[uint64]*QueueChannel),
				processor:      processor,
				globalBucket:   globalBucket,
				bufferSize:     bufferSize,
				user:           nil,
				identifier:     "InvalidTokenQueue",
				isTokenInvalid: invalid,
				botLimit:       limit,
			}, nil
		}

		return nil, err
	}

	identifier := "NoAuth"
	if user != nil {
		queueType = Bot
		identifier = user.Username + "#" + user.Discrim
	}

	if queueType == Bearer {
		identifier = "Bearer"
	}

	ret := &RequestQueue{
		queues:         make(map[uint64]*QueueChannel),
		processor:      processor,
		globalBucket:   globalBucket,
		bufferSize:     bufferSize,
		user:           user,
		identifier:     identifier,
		isTokenInvalid: new(int64),
		botLimit:       limit,
		queueType:      queueType,
	}

	if queueType != Bearer {
		logger.WithFields(logrus.Fields{"globalLimit": limit, "identifier": identifier, "bufferSize": bufferSize}).Info("Created new queue")
		// Only sweep bot queues, bearer queues get completely destroyed and hold way less endpoints
		go ret.tickSweep()
	} else {
		logger.WithFields(logrus.Fields{"globalLimit": limit, "identifier": identifier, "bufferSize": bufferSize}).Debug("Created new bearer queue")
	}

	return ret, nil
}

func (q *RequestQueue) destroy() {
	q.Lock()
	defer q.Unlock()
	logger.Debug("Destroying queue")
	for _, val := range q.queues {
		close(val.ch)
	}
}

func (q *RequestQueue) sweep() {
	q.Lock()
	defer q.Unlock()
	logger.Info("Sweep start")
	sweptEntries := 0
	for key, val := range q.queues {
		if time.Since(val.lastUsed) > 10*time.Minute {
			close(val.ch)
			delete(q.queues, key)
			sweptEntries++
		}
	}
	logger.WithFields(logrus.Fields{"sweptEntries": sweptEntries}).Info("Finished sweep")
}

func (q *RequestQueue) tickSweep() {
	t := time.NewTicker(5 * time.Minute)

	for range t.C {
		q.sweep()
	}
}

var errQueueClosed = errors.New("failed to send due to closed channel, sending 429 for client to retry")

type enqueueResult int

const (
	enqueued enqueueResult = iota
	enqueueClosed
	enqueueCancelled
	enqueueDeadline
)

// enqueue puts the item on its bucket's channel. A full channel used to block
// here with no way out: the client could time out and leave, and the request
// still waited for a slot. Now it gives up when the client does, or when the
// request's deadline passes.
func enqueue(queue *QueueChannel, item *QueueItem) (result enqueueResult) {
	defer func() {
		// Sending on a channel the sweeper closed panics.
		if recover() != nil {
			result = enqueueClosed
		}
	}()

	var deadlineC <-chan time.Time
	if !item.Deadline.IsZero() {
		t := time.NewTimer(time.Until(item.Deadline))
		defer t.Stop()
		deadlineC = t.C
	}

	select {
	case queue.ch <- item:
		return enqueued
	case <-item.Req.Context().Done():
		return enqueueCancelled
	case <-deadlineC:
		return enqueueDeadline
	}
}

func (q *RequestQueue) Queue(req *http.Request, res *http.ResponseWriter, path string, pathHash uint64, priority Priority, deadline time.Time, arrived time.Time) error {
	logger.WithFields(logrus.Fields{
		"bucket": path,
		"path":   req.URL.Path,
		"method": req.Method,
	}).Trace("Inbound request")

	ch := q.getQueueChannel(path, pathHash)

	doneChan := make(chan *http.Response)
	errChan := make(chan error)

	item := &QueueItem{
		Req:      req,
		Res:      res,
		doneChan: doneChan,
		errChan:  errChan,
		Priority: priority,
		Deadline: deadline,
		Arrived:  arrived,
	}

	switch enqueue(ch, item) {
	case enqueueClosed:
		// The previous code sent this error on errChan from inside the
		// recover, before anything was receiving from it - a goroutine leak
		// per occurrence.
		Generate429(res)
		return errQueueClosed
	case enqueueCancelled:
		observeDropped(item, "cancelled", "")
		return req.Context().Err()
	case enqueueDeadline:
		shed(item, ShedReasonQueueDeadline)
		return nil
	}

	select {
	case <-doneChan:
		return nil
	case err := <-errChan:
		return err
	}
}

func (q *RequestQueue) getQueueChannel(path string, pathHash uint64) *QueueChannel {
	t := time.Now()
	q.Lock()
	defer q.Unlock()
	ch, ok := q.queues[pathHash]
	if !ok {
		ch = &QueueChannel{
			ch:       make(chan *QueueItem, q.bufferSize),
			lastUsed: t,
		}
		q.queues[pathHash] = ch
		// It's important that we only have 1 goroutine per channel
		go q.subscribe(ch, path, pathHash)
	} else {
		ch.lastUsed = t
	}
	return ch
}

func parseHeaders(headers *http.Header, preferRetryAfter bool) (int64, int64, time.Duration, bool, error) {
	if headers == nil {
		return 0, 0, 0, false, errors.New("null headers")
	}

	limit := headers.Get("x-ratelimit-limit")
	remaining := headers.Get("x-ratelimit-remaining")
	resetAfter := headers.Get("x-ratelimit-reset-after")
	retryAfter := headers.Get("retry-after")
	if resetAfter == "" || (preferRetryAfter && retryAfter != "") {
		// Globals return no x-ratelimit-reset-after headers, shared ratelimits have a wrong reset-after
		// this is the best option without parsing the body
		resetAfter = headers.Get("retry-after")
	}
	isGlobal := headers.Get("x-ratelimit-global") == "true"

	var resetParsed float64
	var reset time.Duration = 0
	var err error
	if resetAfter != "" {
		resetParsed, err = strconv.ParseFloat(resetAfter, 64)
		if err != nil {
			return 0, 0, 0, false, err
		}

		// Convert to MS instead of seconds to preserve decimal precision
		reset = time.Duration(int(resetParsed*1000)) * time.Millisecond
	}

	if isGlobal {
		return 0, 0, reset, isGlobal, nil
	}

	if limit == "" {
		return 0, 0, reset, false, nil
	}

	limitParsed, err := strconv.ParseInt(limit, 10, 32)
	if err != nil {
		return 0, 0, 0, false, err
	}

	remainingParsed, err := strconv.ParseInt(remaining, 10, 32)
	if err != nil {
		return 0, 0, 0, false, err
	}

	return limitParsed, remainingParsed, reset, isGlobal, nil
}

func return404webhook(item *QueueItem) {
	res := *item.Res
	res.WriteHeader(404)
	body := "{\n  \"message\": \"Unknown Webhook\",\n  \"code\": 10015\n}"
	_, err := res.Write([]byte(body))
	if err != nil {
		item.errChan <- err
		return
	}
	item.doneChan <- nil

}

func return401(item *QueueItem) {
	res := *item.Res
	res.WriteHeader(401)
	body := "{\n\t\"message\": \"401: Unauthorized\",\n\t\"code\": 0\n}"
	_, err := res.Write([]byte(body))
	if err != nil {
		item.errChan <- err
		return
	}
	item.doneChan <- nil
}

func isInteraction(url string) bool {
	parts := strings.Split(strings.SplitN(url, "?", 1)[0], "/")
	for _, p := range parts {
		if len(p) > 128 {
			return true
		}
	}
	return false
}

// observeDropped records a request that will not be sent.
func observeDropped(item *QueueItem, outcome string, reason string) {
	labels := map[string]string{"priority": item.Priority.String(), "outcome": outcome}
	QueueWaitHistogram.With(labels).Observe(time.Since(item.Arrived).Seconds())
	if outcome == "cancelled" {
		CancelledBeforeSendCounter.With(map[string]string{"priority": item.Priority.String()}).Inc()
	} else if reason != "" {
		ShedCounter.With(map[string]string{"reason": reason, "priority": item.Priority.String()}).Inc()
		proxyHealth.observeShed(reason)
	}
}

// shed answers a request that will not be sent, without contacting Discord.
// Only called before the response has been written.
func shed(item *QueueItem, reason string) {
	observeDropped(item, "shed", reason)
	WriteShed(*item.Res, reason)
}

// shedInQueue is shed for an item the subscriber owns: the client goroutine is
// waiting on doneChan.
func shedInQueue(item *QueueItem, reason string) {
	shed(item, reason)
	item.doneChan <- nil
}

// expired reports whether the item's deadline has passed.
func (item *QueueItem) expired(now time.Time) bool {
	return !item.Deadline.IsZero() && !now.Before(item.Deadline)
}

// readyToSend is the last check before a request costs a global token and a
// Discord call. It returns false, having answered the client, when the request
// is no longer worth sending.
//
// The original loop sent everything it dequeued. A request whose client had
// timed out minutes earlier was still given a global token, then failed on
// its cancelled context - so under overload the limiter spent its budget on
// requests nobody was waiting for, and the queue never drained.
func (q *RequestQueue) readyToSend(item *QueueItem) bool {
	if err := item.Req.Context().Err(); err != nil {
		observeDropped(item, "cancelled", "")
		item.errChan <- err
		return false
	}
	if item.expired(time.Now()) {
		shedInQueue(item, ShedReasonQueueDeadline)
		return false
	}
	if q.takeGlobal == nil {
		return true
	}
	err := q.takeGlobal(item.Req.Context(), item)
	switch {
	case err == nil:
		return true
	case errors.Is(err, ErrShed):
		shedInQueue(item, ShedReasonGlobalDeadline)
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		observeDropped(item, "cancelled", "")
		item.errChan <- err
	default:
		// The cluster's global node could not be reached. Same answer as
		// before this change: a generated 429 so the client retries.
		ErrorCounter.Inc()
		Generate429(item.Res)
		item.errChan <- err
	}
	return false
}

func (q *RequestQueue) subscribe(ch *QueueChannel, path string, pathHash uint64) {
	// This function has 1 goroutine for each bucket path
	// Locking here is not needed

	//Only used for logging
	var prevRem int64 = 0
	var prevReset time.Duration = 0

	// Fail fast path for webhook 404s
	var ret404 = false
	for item := range ch.ch {
		ctx := context.WithValue(item.Req.Context(), "identifier", q.identifier)
		if ret404 {
			return404webhook(item)
			continue
		}

		if atomic.LoadInt64(q.isTokenInvalid) > 0 {
			return401(item)
			continue
		}

		if !q.readyToSend(item) {
			continue
		}
		QueueWaitHistogram.With(map[string]string{"priority": item.Priority.String(), "outcome": "sent"}).Observe(time.Since(item.Arrived).Seconds())

		resp, err := q.processor(ctx, item)
		if err != nil {
			item.errChan <- err
			continue
		}

		scope := resp.Header.Get("x-ratelimit-scope")

		_, remaining, resetAfter, isGlobal, err := parseHeaders(&resp.Header, scope != "user")

		if isGlobal {
			// Pause the whole bot's global limiter until Discord's lock
			// expires. The previous code compare-and-swapped a timestamp that
			// nothing ever read, so every bucket kept sending into the lock.
			Global429Counter.Inc()
			until := time.Now().Add(resetAfter)
			if resetAfter > 0 && q.lockGlobal != nil {
				q.lockGlobal(until)
			}
			logger.WithFields(logrus.Fields{
				"until":      until,
				"resetAfter": resetAfter,
			}).Warn("Global reached, locking")
		}

		if err != nil {
			item.errChan <- err
			continue
		}
		item.doneChan <- resp

		if resp.StatusCode == 429 && scope != "shared" {
			logger.WithFields(logrus.Fields{
				"prevRemaining":  prevRem,
				"prevResetAfter": prevReset,
				"remaining":      remaining,
				"resetAfter":     resetAfter,
				"bucket":         path,
				"route":          item.Req.URL.String(),
				"method":         item.Req.Method,
				"isGlobal":       isGlobal,
				"pathHash":       pathHash,
				// TODO: Remove this when 429s are not a problem anymore
				"discordBucket":  resp.Header.Get("x-ratelimit-bucket"),
				"ratelimitScope": resp.Header.Get("x-ratelimit-scope"),
			}).Warn("Unexpected 429")
		}

		if resp.StatusCode == 404 && strings.HasPrefix(path, "/webhooks/") && !isInteraction(item.Req.URL.String()) {
			logger.WithFields(logrus.Fields{
				"bucket": path,
				"route":  item.Req.URL.String(),
				"method": item.Req.Method,
			}).Info("Setting fail fast 404 for webhook")
			ret404 = true
		}

		if resp.StatusCode == 401 && !isInteraction(item.Req.URL.String()) && q.queueType != NoAuth {
			// Permanently lock this queue
			logger.WithFields(logrus.Fields{
				"bucket":     path,
				"route":      item.Req.URL.String(),
				"method":     item.Req.Method,
				"identifier": q.identifier,
				"status":     resp.StatusCode,
			}).Error("Received 401 during normal operation, assuming token is invalidated, locking bucket permanently")

			if EnvGet("DISABLE_401_LOCK", "false") != "true" {
				atomic.StoreInt64(q.isTokenInvalid, 999)
			}
		}

		// Prevent reaction bucket from being stuck
		if resp.StatusCode == 429 && scope == "shared" && (path == "/channels/!/messages/!/reactions/!modify" || path == "/channels/!/messages/!/reactions/!/!") {
			prevRem, prevReset = remaining, resetAfter
			continue
		}

		if remaining == 0 || resp.StatusCode == 429 {
			duration := time.Until(time.Now().Add(resetAfter))
			time.Sleep(duration)
		}
		prevRem, prevReset = remaining, resetAfter
	}
}
