package queue

import (
	"container/list"
	"time"
)

type querierUserQueues interface {
	isEmpty() bool
	getOrAddQueue(userID string, maxQueriers int) *list.List
	deleteQueue(userID string)
	getNextQueueForQuerier(lastUserIndex int, querierID string) (*list.List, string, int)
	addQuerierConnection(querierID string)
	removeQuerierConnection(querierID string, now time.Time)
	notifyQuerierShutdown(querierID string)
	forgetDisconnectedQueriers(now time.Time) int
}

//type listLike interface {
//	Len() int
//	Front() *list.Element
//	Remove(e *list.Element) any
//	PushBack(v any) *list.Element
//}

// impl querierUserQueues with TreeQueues
type userTreeQueues struct {
	treeQueues *TreeQueue
}

// len
func (utq userTreeQueues) isEmpty() bool {
	return utq.treeQueues.isEmpty()
}

func (utq userTreeQueues) getOrAddQueue(userID string, maxQueriers int) *list.List {
	//TODO implement me
	panic("implement me")
}

func (utq userTreeQueues) deleteQueue(userID string) {
	//TODO implement me
	panic("implement me")
}

func (utq userTreeQueues) getNextQueueForQuerier(lastUserIndex int, querierID string) (*list.List, string, int) {
	//TODO implement me
	panic("implement me")
}

func (utq userTreeQueues) addQuerierConnection(querierID string) {
	//TODO implement me
	panic("implement me")
}

func (utq userTreeQueues) removeQuerierConnection(querierID string, now time.Time) {
	//TODO implement me
	panic("implement me")
}

func (utq userTreeQueues) notifyQuerierShutdown(querierID string) {
	//TODO implement me
	panic("implement me")
}

func (utq userTreeQueues) forgetDisconnectedQueriers(now time.Time) int {
	//TODO implement me
	panic("implement me")
}
