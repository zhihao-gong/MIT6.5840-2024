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
type Task struct {
	Id          string
	InputFiles  []string
	OutputFiles []string
	Type        TaskType

	AssignedWorkerId string
	AssignedTime     int64
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
	pending  *utils.SafeMap[Task]
	assigned *utils.SafeMap[Task]
	finished *utils.SafeMap[Task]
}

func NewTaskSet(total int64, pending *utils.SafeMap[Task]) *TaskSet {
	return &TaskSet{
		total:    total,
		pending:  pending,
		assigned: utils.NewSafeMap[Task](),
		finished: utils.NewSafeMap[Task](),
	}
}

// Get a task from the pending queue
func (ts *TaskSet) Get(workerId string) *Task {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	task := ts.pending.GetOne()
	if task == nil {
		return nil
	}

	task.AssignedWorkerId = workerId
	task.AssignedTime = time.Now().Unix()

	ts.pending.Delete(task.Id)
	ts.assigned.Put(task.Id, *task)

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
