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

type Phase int

const (
	MapPhaseType Phase = iota
	ReducePhaseType
	DonePhaseType
)

type TaskManager struct {
	mapTasks    *TaskSet
	reduceTasks *TaskSet

	phase Phase
	mutex sync.RWMutex
}

func newTaskManager(mapTasks *utils.SafeMap[task]) *TaskManager {
	return &TaskManager{
		mapTasks:    newTaskSet(int64(mapTasks.Len()), mapTasks),
		reduceTasks: nil,
		phase:       MapPhaseType,
	}
}

func (tm *TaskManager) scheduleTask(workerId string) *task {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if tm.phase == MapPhaseType {
		return tm.mapTasks.GetPending(workerId)
	} else {
		return tm.reduceTasks.GetPending(workerId)
	}
}

func (tm *TaskManager) setFinished(taskId string) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if tm.phase == MapPhaseType {
		success := tm.mapTasks.SetFinished(taskId)
		if success && tm.mapTasks.AllFinished() {
			tm.phase = ReducePhaseType
		}
		return success
	} else {
		success := tm.reduceTasks.SetFinished(taskId)
		if success && tm.reduceTasks.AllFinished() {
			tm.phase = DonePhaseType
		}
		return success
	}
}

func (tm *TaskManager) setPending(taskId string) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if tm.phase == MapPhaseType {
		return tm.mapTasks.SetPending(taskId)
	} else {
		return tm.reduceTasks.SetPending(taskId)
	}
}

type TaskSet struct {
	total int64

	pending  *utils.SafeMap[task]
	assigned *utils.SafeMap[task]
	finished *utils.SafeMap[task]
}

func newTaskSet(total int64, tasks *utils.SafeMap[task]) *TaskSet {
	return &TaskSet{
		total:    total,
		pending:  tasks,
		assigned: utils.NewSafeMap[task](),
		finished: utils.NewSafeMap[task](),
	}
}

// Get a task from the pending queue
func (ts *TaskSet) GetPending(workerId string) *task {
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

// Set a task to finished
func (ts *TaskSet) SetFinished(taskId string) bool {
	task, ok := ts.assigned.Get(taskId)
	if !ok {
		return false
	}

	ts.assigned.Delete(taskId)
	ts.finished.Put(taskId, task)

	return true
}

// Set a task to pending
func (ts *TaskSet) SetPending(taskId string) bool {
	task, ok := ts.assigned.Get(taskId)
	if !ok {
		return false
	}

	ts.assigned.Delete(taskId)
	ts.pending.Put(taskId, task)

	return true
}

func (ts *TaskSet) AllFinished() bool {
	return ts.finished.Len() == int(ts.total)
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
