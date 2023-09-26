// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/queue.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"container/list"
	"context"
	"fmt"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

const (
	// How frequently to check for disconnected queriers that should be forgotten.
	forgetCheckPeriod = 5 * time.Second
)

var (
	ErrTooManyRequests     = errors.New("too many outstanding requests")
	ErrStopped             = errors.New("queue is stopped")
	ErrQuerierShuttingDown = errors.New("querier has informed the scheduler it is shutting down")
)

// UserIndex is opaque type that allows to resume iteration over users between successive calls
// of RequestQueue.GetNextRequestForQuerier method.
type UserIndex struct {
	last int
}

// Modify index to start iteration on the same user, for which last queue was returned.
func (ui UserIndex) ReuseLastUser() UserIndex {
	if ui.last >= 0 {
		return UserIndex{last: ui.last - 1}
	}
	return ui
}

// FirstUser returns UserIndex that starts iteration over user queues from the very first user.
func FirstUser() UserIndex {
	return UserIndex{last: -1}
}

// Request stored into the queue.
type Request interface{}

// RequestQueue holds incoming requests in per-user queues. It also assigns each user specified number of queriers,
// and when querier asks for next request to handle (using GetNextRequestForQuerier), it returns requests
// in a fair fashion.
type RequestQueue struct {
	services.Service

	maxOutstandingPerTenant int
	forgetDelay             time.Duration

	connectedQuerierWorkers *atomic.Int32

	stopRequested               chan struct{} // Written to by stop() to wake up dispatcherLoop() in response to a stop request.
	stopCompleted               chan struct{} // Closed by dispatcherLoop() after a stop is requested and the dispatcher has stopped.
	querierOperations           chan querierOperation
	enqueueRequests             chan enqueueRequest
	availableQuerierConnections chan *querierConnection

	queueLength       *prometheus.GaugeVec   // Per user and reason.
	discardedRequests *prometheus.CounterVec // Per user.

	enqueueDuration prometheus.Histogram
}

type querierOperation struct {
	querierID string
	operation querierOperationType
}

type querierOperationType int

const (
	registerConnection querierOperationType = iota
	unregisterConnection
	notifyShutdown
	forgetDisconnected
)

type enqueueRequest struct {
	userID      string
	req         Request
	maxQueriers int
	successFn   func()
	processed   chan error
}

func NewRequestQueue(maxOutstandingPerTenant int, forgetDelay time.Duration, queueLength *prometheus.GaugeVec, discardedRequests *prometheus.CounterVec, enqueueDuration prometheus.Histogram) *RequestQueue {
	q := &RequestQueue{
		maxOutstandingPerTenant: maxOutstandingPerTenant,
		forgetDelay:             forgetDelay,
		connectedQuerierWorkers: atomic.NewInt32(0),
		queueLength:             queueLength,
		discardedRequests:       discardedRequests,
		enqueueDuration:         enqueueDuration,

		stopRequested: make(chan struct{}),
		stopCompleted: make(chan struct{}),

		// These channels must not be buffered so that we can detect when dispatcherLoop() has finished.
		querierOperations:           make(chan querierOperation),
		enqueueRequests:             make(chan enqueueRequest),
		availableQuerierConnections: make(chan *querierConnection),
	}

	q.Service = services.NewTimerService(forgetCheckPeriod, q.starting, q.forgetDisconnectedQueriers, q.stop).WithName("request queue")

	return q
}

func (q *RequestQueue) starting(_ context.Context) error {

	go q.dispatcherLoop()

	return nil
}

func (q *RequestQueue) dispatcherLoop() {
	stopping := false
	queues := newUserQueues(q.maxOutstandingPerTenant, q.forgetDelay)
	waitingQuerierConnections := list.New()

	for {
		needToDispatchQueries := false

		select {
		case <-q.stopRequested:
			// Nothing much to do here - fall through to the stop logic below to see if we can stop immediately.
			stopping = true
		case qe := <-q.querierOperations:
			// These operations may cause a resharding, so we should always try to dispatch queries afterwards.
			// In the future, we could make this smarter: detect when a resharding actually happened and only trigger dispatching queries in those cases.
			switch qe.operation {
			case registerConnection:
				q.connectedQuerierWorkers.Inc()
				queues.tenantQuerierState.addQuerierConnection(qe.querierID)
				needToDispatchQueries = true
			case unregisterConnection:
				q.connectedQuerierWorkers.Dec()
				queues.tenantQuerierState.removeQuerierConnection(qe.querierID, time.Now())
				needToDispatchQueries = true
			case notifyShutdown:
				queues.tenantQuerierState.notifyQuerierShutdown(qe.querierID)
				needToDispatchQueries = true

				// Tell any waiting GetNextRequestForQuerier calls for this querier that nothing is coming.
				// If the querier shuts down without notifying us, this is OK: we'll never mark it as shutting down, so we'll
				// dispatch a query to GetNextRequestForQuerier and Scheduler.QuerierLoop will try to send the query to it
				// later. This will fail because the connection is broken, and GetNextRequestForQuerier won't be called again.
				q.cancelWaitingConnectionsForQuerier(qe.querierID, waitingQuerierConnections)
			case forgetDisconnected:
				if queues.tenantQuerierState.forgetDisconnectedQueriers(time.Now()) > 0 {
					// Removing some queriers may have caused a resharding.
					needToDispatchQueries = true
				}
			default:
				panic(fmt.Sprintf("received unknown querier event %v for querier ID %v", qe.operation, qe.querierID))
			}
		case r := <-q.enqueueRequests:
			err := q.handleEnqueueRequest(queues, r)
			r.processed <- err

			if err == nil {
				needToDispatchQueries = true
			}
		case querierConn := <-q.availableQuerierConnections:
			if !q.dispatchRequestToQuerier(queues, querierConn) {
				// No requests available for this querier connection right now. Add it to the list to try later.
				querierConn.element = waitingQuerierConnections.PushBack(querierConn)
			}
		}

		if needToDispatchQueries {
			currentElement := waitingQuerierConnections.Front()

			for currentElement != nil && queues.len() > 0 {
				querierConn := currentElement.Value.(*querierConnection)
				nextElement := currentElement.Next() // We have to capture the next element before calling Remove(), as Remove() clears it.

				if q.dispatchRequestToQuerier(queues, querierConn) {
					waitingQuerierConnections.Remove(currentElement)
				}

				currentElement = nextElement
			}
		}

		if stopping && (queues.len() == 0 || q.connectedQuerierWorkers.Load() == 0) {
			// Tell any waiting GetNextRequestForQuerier calls that nothing is coming.
			currentElement := waitingQuerierConnections.Front()

			for currentElement != nil {
				querierConn := currentElement.Value.(*querierConnection)
				querierConn.sendError(ErrStopped)
				currentElement = currentElement.Next()
			}

			// We are done.
			close(q.stopCompleted)
			return
		}
	}
}

func (q *RequestQueue) handleEnqueueRequest(queues *queues, r enqueueRequest) error {
	queue := queues.getOrAddTenantQueue(r.userID, r.maxQueriers)
	if queue == nil {
		// This can only happen if userID is "".
		return errors.New("no queue found")
	}

	if queue.Len()+1 > queues.maxUserQueueSize {
		q.discardedRequests.WithLabelValues(r.userID).Inc()
		return ErrTooManyRequests
	}

	queue.PushBack(r.req)
	q.queueLength.WithLabelValues(r.userID).Inc()

	// Call the successFn here to ensure we call it before sending this request to a waiting querier.
	if r.successFn != nil {
		r.successFn()
	}

	return nil
}

// dispatchRequestToQuerier finds and forwards a request to a querier, if a suitable request is available.
// Returns true if this querier should be removed from the list of waiting queriers (eg. because a request has been forwarded to it), false otherwise.
func (q *RequestQueue) dispatchRequestToQuerier(queues *queues, querierConn *querierConnection) bool {
	// If this querier has told us it's shutting down, don't bother trying to find a query request for it.
	// Terminate GetNextRequestForQuerier with an error now.
	queue, userID, idx, err := queues.getNextQueueForQuerier(querierConn.lastUserIndex.last, querierConn.querierID)
	if err != nil {
		querierConn.sendError(err)
		return true
	}

	querierConn.lastUserIndex.last = idx
	if queue == nil {
		// Nothing available for this querier, try again next time.
		return false
	}

	// Pick next request from the queue. The queue is guaranteed not to be empty because we remove empty queues.
	queueElement := queue.Front()

	requestSent := querierConn.send(nextRequestForQuerier{
		req:           queueElement.Value,
		lastUserIndex: querierConn.lastUserIndex,
		err:           nil,
	})

	if requestSent {
		// If GetNextRequestForQuerier received the request, remove it from the queue.
		// (GetNextRequestForQuerier might have already returned if its context was cancelled.)
		queue.Remove(queueElement)

		if queue.Len() == 0 {
			queues.deleteQueue(userID)
		}

		q.queueLength.WithLabelValues(userID).Dec()
	}

	return true
}

func (q *RequestQueue) cancelWaitingConnectionsForQuerier(querierID string, waitingQuerierConnections *list.List) {
	currentElement := waitingQuerierConnections.Front()

	for currentElement != nil {
		querierConn := currentElement.Value.(*querierConnection)
		nextElement := currentElement.Next() // We have to capture the next element before calling Remove(), as Remove() clears it.

		if querierConn.querierID == querierID {
			querierConn.sendError(ErrQuerierShuttingDown)
			waitingQuerierConnections.Remove(currentElement)
		}

		currentElement = nextElement
	}
}

// EnqueueRequest puts the request into the queue. maxQueries is user-specific value that specifies how many queriers can
// this user use (zero or negative = all queriers). It is passed to each EnqueueRequest, because it can change
// between calls.
//
// If request is successfully enqueued, successFn is called before any querier can receive the request.
func (q *RequestQueue) EnqueueRequest(userID string, req Request, maxQueriers int, successFn func()) error {
	start := time.Now()
	defer func() {
		q.enqueueDuration.Observe(time.Since(start).Seconds())
	}()

	r := enqueueRequest{
		userID:      userID,
		req:         req,
		maxQueriers: maxQueriers,
		successFn:   successFn,
		processed:   make(chan error),
	}

	select {
	case q.enqueueRequests <- r:
		return <-r.processed
	case <-q.stopCompleted:
		return ErrStopped
	}
}

// GetNextRequestForQuerier find next user queue and takes the next request off of it. Will block if there are no requests.
// By passing user index from previous call of this method, querier guarantees that it iterates over all users fairly.
// If querier finds that request from the user is already expired, it can get a request for the same user by using UserIndex.ReuseLastUser.
func (q *RequestQueue) GetNextRequestForQuerier(ctx context.Context, last UserIndex, querierID string) (Request, UserIndex, error) {
	querierConn := &querierConnection{
		ctx:           ctx,
		querierID:     querierID,
		lastUserIndex: last,
		processed:     make(chan nextRequestForQuerier),
	}

	select {
	case q.availableQuerierConnections <- querierConn:
		// The dispatcher now knows we're waiting. Either we'll get a request to send to a querier, or we'll cancel.
		select {
		case result := <-querierConn.processed:
			return result.req, result.lastUserIndex, result.err
		case <-ctx.Done():
			return nil, last, ctx.Err()
		}
	case <-ctx.Done():
		return nil, last, ctx.Err()
	case <-q.stopCompleted:
		return nil, last, ErrStopped
	}
}

func (q *RequestQueue) stop(_ error) error {
	q.stopRequested <- struct{}{} // Why not close the channel? We only want to trigger dispatcherLoop() once.
	<-q.stopCompleted

	return nil
}

func (q *RequestQueue) forgetDisconnectedQueriers(_ context.Context) error {
	q.runQuerierOperation("", forgetDisconnected)

	return nil
}

func (q *RequestQueue) RegisterQuerierConnection(querierID string) {
	q.runQuerierOperation(querierID, registerConnection)
}

func (q *RequestQueue) UnregisterQuerierConnection(querierID string) {
	q.runQuerierOperation(querierID, unregisterConnection)
}

func (q *RequestQueue) NotifyQuerierShutdown(querierID string) {
	q.runQuerierOperation(querierID, notifyShutdown)
}

func (q *RequestQueue) runQuerierOperation(querierID string, operation querierOperationType) {
	op := querierOperation{
		querierID: querierID,
		operation: operation,
	}

	select {
	case q.querierOperations <- op:
		// The dispatcher has received the operation. There's nothing more to do.
	case <-q.stopCompleted:
		// The dispatcher stopped before it could process the operation. There's nothing more to do.
	}
}

func (q *RequestQueue) GetConnectedQuerierWorkersMetric() float64 {
	return float64(q.connectedQuerierWorkers.Load())
}

type querierConnection struct {
	ctx           context.Context
	querierID     string
	lastUserIndex UserIndex
	processed     chan nextRequestForQuerier

	haveUsed bool // Must be set to true after sending a message to processed, to ensure we only ever try to send one message to processed.
	element  *list.Element
}

func (q *querierConnection) sendError(err error) {
	// If GetNextRequestForQuerier is already gone, we don't care, so ignore the result from send.
	_ = q.send(nextRequestForQuerier{err: err})
}

// send sends req to the GetNextRequestForQuerier call that is waiting for a new query.
// Returns true if sending succeeds, or false otherwise (eg. because the GetNextRequestForQuerier call has already returned due to a context
// cancellation).
func (q *querierConnection) send(req nextRequestForQuerier) bool {
	if q.haveUsed {
		panic("bug: should not try to send multiple messages to a querier")
	}

	q.haveUsed = true
	defer close(q.processed)

	select {
	case q.processed <- req:
		return true
	case <-q.ctx.Done():
		return false
	}
}

type nextRequestForQuerier struct {
	req           Request
	lastUserIndex UserIndex
	err           error
}
