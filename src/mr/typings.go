package mr

import (
	"sync"
	"time"

	"6.5840/utils"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
type task struct {
	id string

	taskType TaskType
	inputs   []string

	nReduce          int
	assignedWorkerId string
	assignedTime     int64
}

type worker struct {
	id           string
	lastPingTime int64
	status       WorkerStatus
}

type Phase int

const (
	MapPhaseType Phase = iota
	ReducePhaseType
)

type TaskSet struct {
	total int64

	mutex    sync.RWMutex
	pending  *utils.SafeMap[task]
	assigned *utils.SafeMap[task]
	finished *utils.SafeMap[task]
}

func NewTaskSet(total int64, pending *utils.SafeMap[task]) *TaskSet {
	return &TaskSet{
		total:    total,
		pending:  pending,
		assigned: utils.NewSafeMap[task](),
		finished: utils.NewSafeMap[task](),
	}
}

// Get a task from the pending queue
func (ts *TaskSet) GetPendingTask(workerId string) *task {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	task := ts.pending.GetOne()
	if task == nil {
		return nil
	}

	task.assignedWorkerId = workerId
	task.assignedTime = time.Now().Unix()

	ts.pending.Delete(task.id)
	ts.assigned.Put(task.id, *task)

	return task
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
