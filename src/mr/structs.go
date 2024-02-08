package mr

import (
	"sync"

	"6.5840/utils"
)

// Map functions return a slice of KeyValue.
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
	Idle WorkerStatus = iota
	InProgress
	Completed
	Lost
)

type TaskType int

const (
	MapTaskType TaskType = iota
	ReduceTaskType
)

// Task is the unit of work for the worker
// Captialize the first letter of the fields to make them public to rpc modules
type task struct {
	Id string

	TaskType TaskType
	Inputs   []string
	Outputs  []string

	NReduce          int
	AssignedWorkerId string
	AssignedTime     int64
}

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
