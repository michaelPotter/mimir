// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"container/list"
	"math/rand"
	"sort"
	"time"

	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/util"
)

const emptyTenantID = ""

type querierConn struct {
	// Number of active connections.
	connections int

	// True if the querier notified it's gracefully shutting down.
	shuttingDown bool

	// When the last connection has been unregistered.
	disconnectedAt time.Time
}

type tenantQuerierAssignments struct {
	// a tenant has many queriers
	// a tenant has *all* queriers if:
	//  - sharding is disabled (max-queriers-per-tenant=0)
	//  - or if max-queriers-per-tenant >= the number of queriers
	//
	// Tenant -> Queriers is the core relationship randomized from the shuffle shard seed.
	// The shuffle shard seed is itself consistently hashed from the tenant ID.
	// However, the most common operation is the querier asking for its next request,
	// which requires a relatively efficient lookup or check of Querier -> Tenant.
	//
	// Reshuffling is done when:
	//  - a querier connection is added or removed
	//  - it is detected during request enqueueing that a tenant's queriers
	//    were calculated from an outdated max-queriers-per-tenant value

	queriersByID map[string]*querierConn
	// Sorted list of querier ids, used when shuffle sharding queriers for tenant
	querierIDsSorted []string

	// How long to wait before removing a querier which has got disconnected
	// but hasn't notified about a graceful shutdown.
	querierForgetDelay time.Duration

	// List of all tenants with queues, used for iteration when searching for next queue to handle.
	tenantIDOrder []string
	tenantsByID   map[string]*queueTenant

	// Tenant assigned querier ID set as determined by shuffle sharding.
	// If tenant querier ID set is not nil, only those queriers can handle the tenant's requests,
	// Tenant querier ID is set to nil if sharding is off or available queriers <= tenant's maxQueriers.
	tenantQuerierIDs map[string]map[string]struct{}
}

type queueTenant struct {
	// seed for shuffle sharding of queriers; computed from userID only,
	// and is therefore consistent between different frontends.
	shuffleShardSeed int64

	// points up to user order to enable efficient removal
	orderIndex int

	maxQueriers int
}

// This struct holds user queues for pending requests. It also keeps track of connected queriers,
// and mapping between users and queriers.
type queues struct {
	tenantQueues map[string]*userQueue

	tenantQuerierAssignments tenantQuerierAssignments

	maxUserQueueSize int
}

type userQueue struct {
	requests *list.List
}

func newUserQueues(maxUserQueueSize int, forgetDelay time.Duration) *queues {
	return &queues{
		tenantQueues: map[string]*userQueue{},
		tenantQuerierAssignments: tenantQuerierAssignments{
			queriersByID:       map[string]*querierConn{},
			querierIDsSorted:   nil,
			querierForgetDelay: forgetDelay,
			tenantIDOrder:      nil,
			tenantsByID:        map[string]*queueTenant{},
			tenantQuerierIDs:   map[string]map[string]struct{}{},
		},
		maxUserQueueSize: maxUserQueueSize,
	}
}

func (q *queues) len() int {
	return len(q.tenantQueues)
}

// Returns existing or new queue for user.
// MaxQueriers is used to compute which queriers should handle requests for this user.
// If maxQueriers is <= 0, all queriers can handle this user's requests.
// If maxQueriers has changed since the last call, queriers for this are recomputed.
func (q *queues) getOrAddTenantQueue(tenantID string, maxQueriers int) *list.List {
	tenant := q.tenantQuerierAssignments.getOrAddTenant(tenantID, maxQueriers)
	if tenant == nil {
		return nil
	}
	queue := q.tenantQueues[tenantID]

	if queue == nil {
		queue = &userQueue{
			requests: list.New(),
		}
		q.tenantQueues[tenantID] = queue
	}

	return queue.requests
}

// Finds next queue for the querier. To support fair scheduling between users, client is expected
// to pass last user index returned by this function as argument. If there was no previous
// last user index, use -1.
func (q *queues) getNextQueueForQuerier(lastUserIndex int, querierID string) (*list.List, string, int, error) {
	nextTenantID, nextTenantIndex, err := q.tenantQuerierAssignments.getNextTenantIDForQuerier(lastUserIndex, querierID)
	if err != nil || nextTenantID == emptyTenantID {
		return nil, nextTenantID, nextTenantIndex, err
	}

	tenantQueue := q.tenantQueues[nextTenantID]
	return tenantQueue.requests, nextTenantID, nextTenantIndex, nil
}

func (q *queues) addQuerierConnection(querierID string) {
	q.tenantQuerierAssignments.addQuerierConnection(querierID)
}

func (q *queues) removeQuerierConnection(querierID string, now time.Time) {
	q.tenantQuerierAssignments.removeQuerierConnection(querierID, now)
}

func (q *queues) notifyQuerierShutdown(querierID string) {
	q.tenantQuerierAssignments.notifyQuerierShutdown(querierID)
}

func (q *queues) forgetDisconnectedQueriers(now time.Time) int {
	return q.tenantQuerierAssignments.forgetDisconnectedQueriers(now)
}

func (q *queues) deleteQueue(tenantID string) {
	tenantQueue := q.tenantQueues[tenantID]
	if tenantQueue == nil {
		return
	}
	delete(q.tenantQueues, tenantID)
	q.tenantQuerierAssignments.removeTenant(tenantID)
}

func (tqa *tenantQuerierAssignments) getNextTenantIDForQuerier(lastTenantIndex int, querierID string) (string, int, error) {
	// check if querier is registered and is not shutting down
	if q := tqa.queriersByID[querierID]; q == nil || q.shuttingDown {
		return emptyTenantID, lastTenantIndex, ErrQuerierShuttingDown
	}
	tenantOrderIndex := lastTenantIndex
	for iters := 0; iters < len(tqa.tenantIDOrder); iters++ {
		tenantOrderIndex++
		if tenantOrderIndex >= len(tqa.tenantIDOrder) {
			tenantOrderIndex = 0
		}

		tenantID := tqa.tenantIDOrder[tenantOrderIndex]
		if tenantID == emptyTenantID {
			continue
		}

		tenantQuerierSet := tqa.tenantQuerierIDs[tenantID]
		if tenantQuerierSet == nil {
			// tenant can use all queriers
			return tenantID, tenantOrderIndex, nil
		} else if _, ok := tenantQuerierSet[querierID]; ok {
			// tenant is assigned this querier
			return tenantID, tenantOrderIndex, nil
		}
	}

	return emptyTenantID, lastTenantIndex, nil
}

func (tqa *tenantQuerierAssignments) getOrAddTenant(tenantID string, maxQueriers int) *queueTenant {
	// empty tenantID is not allowed; "" is used for free spot
	if tenantID == emptyTenantID {
		return nil
	}

	if maxQueriers < 0 {
		maxQueriers = 0
	}

	tenant := tqa.tenantsByID[tenantID]

	if tenant == nil {
		tenant = &queueTenant{
			shuffleShardSeed: util.ShuffleShardSeed(tenantID, ""),
			orderIndex:       -1,
			maxQueriers:      0,
		}
		for i, id := range tqa.tenantIDOrder {
			if id == emptyTenantID {
				// previously removed tenant not yet cleaned up; take its place
				tenant.orderIndex = i
				tqa.tenantIDOrder[i] = tenantID
				tqa.tenantsByID[tenantID] = tenant
				break
			}
		}

		if tenant.orderIndex < 0 {
			// no empty spaces in tenant order; append
			tenant.orderIndex = len(tqa.tenantIDOrder)
			tqa.tenantIDOrder = append(tqa.tenantIDOrder, tenantID)
			tqa.tenantsByID[tenantID] = tenant
		}
	}

	// tenant now either retrieved or created;
	// tenant queriers need computed for new tenant if sharding enabled,
	// or if the tenant already existed but its maxQueriers has changed
	if tenant.maxQueriers != maxQueriers {
		tenant.maxQueriers = maxQueriers
		tqa.shuffleTenantQueriers(tenantID, nil)
	}
	return tenant
}

func (tqa *tenantQuerierAssignments) addQuerierConnection(querierID string) {
	querier := tqa.queriersByID[querierID]
	if querier != nil {
		querier.connections++

		// Reset in case the querier re-connected while it was in the forget waiting period.
		querier.shuttingDown = false
		querier.disconnectedAt = time.Time{}

		return
	}

	// First connection from this querier.
	tqa.queriersByID[querierID] = &querierConn{connections: 1}
	tqa.querierIDsSorted = append(tqa.querierIDsSorted, querierID)
	slices.Sort(tqa.querierIDsSorted)

	tqa.recomputeTenantQueriers()
}

func (tqa *tenantQuerierAssignments) removeTenant(tenantID string) {
	tenant := tqa.tenantsByID[tenantID]
	delete(tqa.tenantsByID, tenantID)
	tqa.tenantIDOrder[tenant.orderIndex] = emptyTenantID

	// Shrink tenant list if possible by removing empty tenant IDs.
	// We remove only from the end; removing from the middle would re-index all tenant IDs
	// and skip tenants when starting iteration from a querier-provided lastTenantIndex.
	// Empty tenant IDs stuck in the middle of the slice are handled
	// by replacing them when a new tenant ID arrives in the queue.
	for i := len(tqa.tenantIDOrder) - 1; i >= 0 && tqa.tenantIDOrder[i] == emptyTenantID; i-- {
		tqa.tenantIDOrder = tqa.tenantIDOrder[:i]
	}
}

func (tqa *tenantQuerierAssignments) removeQuerierConnection(querierID string, now time.Time) {
	querier := tqa.queriersByID[querierID]
	if querier == nil || querier.connections <= 0 {
		panic("unexpected number of connections for querier")
	}

	// Decrease the number of active connections.
	querier.connections--
	if querier.connections > 0 {
		return
	}

	// There no more active connections. If the forget delay is configured then
	// we can remove it only if querier has announced a graceful shutdown.
	if querier.shuttingDown || tqa.querierForgetDelay == 0 {
		tqa.removeQuerier(querierID)
		return
	}

	// No graceful shutdown has been notified yet, so we should track the current time
	// so that we'll remove the querier as soon as we receive the graceful shutdown
	// notification (if any) or once the threshold expires.
	querier.disconnectedAt = now
}

func (tqa *tenantQuerierAssignments) removeQuerier(querierID string) {
	delete(tqa.queriersByID, querierID)

	ix := sort.SearchStrings(tqa.querierIDsSorted, querierID)
	if ix >= len(tqa.querierIDsSorted) || tqa.querierIDsSorted[ix] != querierID {
		panic("incorrect state of sorted queriers")
	}

	tqa.querierIDsSorted = append(tqa.querierIDsSorted[:ix], tqa.querierIDsSorted[ix+1:]...)

	tqa.recomputeTenantQueriers()
}

// notifyQuerierShutdown records that a querier has sent notification about a graceful shutdown.
func (tqa *tenantQuerierAssignments) notifyQuerierShutdown(querierID string) {
	querier := tqa.queriersByID[querierID]
	if querier == nil {
		// The querier may have already been removed, so we just ignore it.
		return
	}

	// If there are no more connections, we should remove the querier.
	if querier.connections == 0 {
		tqa.removeQuerier(querierID)
		return
	}

	// Otherwise we should annotate we received a graceful shutdown notification
	// and the querier will be removed once all connections are unregistered.
	querier.shuttingDown = true
}

// forgetDisconnectedQueriers removes all disconnected queriers that have gone since at least
// the forget delay. Returns the number of forgotten queriers.
func (tqa *tenantQuerierAssignments) forgetDisconnectedQueriers(now time.Time) int {
	// Nothing to do if the forget delay is disabled.
	if tqa.querierForgetDelay == 0 {
		return 0
	}

	// Remove all queriers with no connections that have gone since at least the forget delay.
	threshold := now.Add(-tqa.querierForgetDelay)
	forgotten := 0

	for querierID := range tqa.queriersByID {
		if querier := tqa.queriersByID[querierID]; querier.connections == 0 && querier.disconnectedAt.Before(threshold) {
			tqa.removeQuerier(querierID)
			forgotten++
		}
	}

	return forgotten
}

func (tqa *tenantQuerierAssignments) recomputeTenantQueriers() {
	// Only allocate the scratchpad the first time we need it.
	// If shuffle-sharding is disabled, it will not be used.
	var scratchpad []string

	for tenantID, tenant := range tqa.tenantsByID {
		if tenant.maxQueriers > 0 && tenant.maxQueriers < len(tqa.querierIDsSorted) && scratchpad == nil {
			scratchpad = make([]string, 0, len(tqa.querierIDsSorted))
		}

		tqa.shuffleTenantQueriers(tenantID, scratchpad)
	}
}

func (tqa *tenantQuerierAssignments) shuffleTenantQueriers(tenantID string, scratchpad []string) {
	tenant := tqa.tenantsByID[tenantID]
	if tenant == nil {
		return
	}

	if tenant.maxQueriers == 0 || len(tqa.querierIDsSorted) <= tenant.maxQueriers {
		// shuffle shard is either disabled or calculation is unnecessary
		tqa.tenantQuerierIDs[tenantID] = nil
		return
	}

	querierIDSet := make(map[string]struct{}, tenant.maxQueriers)
	rnd := rand.New(rand.NewSource(tenant.shuffleShardSeed))

	scratchpad = append(scratchpad[:0], tqa.querierIDsSorted...)

	last := len(scratchpad) - 1
	for i := 0; i < tenant.maxQueriers; i++ {
		r := rnd.Intn(last + 1)
		querierIDSet[scratchpad[r]] = struct{}{}
		// move selected item to the end, it won't be selected anymore.
		scratchpad[r], scratchpad[last] = scratchpad[last], scratchpad[r]
		last--
	}
	tqa.tenantQuerierIDs[tenantID] = querierIDSet
}
