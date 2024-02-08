package mr

import (
	"sync"

	"6.5840/utils"
)

// KeyValue returned by map functions
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type byKey []KeyValue

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerStatus int

const (
	Normal WorkerStatus = iota
	Lost
)

type worker struct {
	id           string
	lastPingTime int64
	status       WorkerStatus
}

type WorkerSet struct {
	mutex   sync.RWMutex
	mapping *utils.SafeMap[worker]
}

func NewWorkerSet() *WorkerSet {
	return &WorkerSet{
		mapping: utils.NewSafeMap[worker](),
	}
}
