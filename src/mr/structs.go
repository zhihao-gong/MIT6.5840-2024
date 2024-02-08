package mr

import (
	"strconv"
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
	nReduce     int
	mapTasks    *TaskSet
	reduceTasks *TaskSet

	phase Phase
	mutex sync.RWMutex
}

func newTaskManager(nReduce int, mapTasks *utils.SafeMap[task]) *TaskManager {
	return &TaskManager{
		nReduce:     nReduce,
		mapTasks:    newTaskSet(int(mapTasks.Len()), mapTasks),
		reduceTasks: nil,
		phase:       MapPhaseType,
	}
}

func (tm *TaskManager) scheduleTask(workerId string) *task {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	switch tm.phase {
	case MapPhaseType:
		return tm.mapTasks.GetPending(workerId)
	case ReducePhaseType:
		return tm.reduceTasks.GetPending(workerId)
	default:
		return nil
	}
}

func (tm *TaskManager) setFinished(taskId string, outputs []string) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if tm.phase == MapPhaseType {
		success := tm.mapTasks.SetFinished(taskId, outputs)
		if success && tm.mapTasks.AllFinished() {
			tm.phase = ReducePhaseType
		}
		tm.reduceTasks = newTaskSet(tm.nReduce, tm.initReduceTasks())
		return success
	} else {
		success := tm.reduceTasks.SetFinished(taskId, outputs)
		if success && tm.reduceTasks.AllFinished() {
			tm.phase = DonePhaseType
		}
		return success
	}
}

func (tm *TaskManager) initReduceTasks() *utils.SafeMap[task] {
	reduceTasks := utils.NewSafeMap[task]()
	for i := 0; i < tm.nReduce; i++ {
		id := strconv.Itoa(i)

		j := 0
		InputFiles := make([]string, tm.mapTasks.total)
		for _, t := range tm.mapTasks.finished.Values() {
			InputFiles[j] = t.Outputs[i]
			j++
		}

		reduceTasks.Put(id, task{
			Id:       id,
			TaskType: ReduceTaskType,
			Inputs:   InputFiles,
		})
	}

	return reduceTasks
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
	total int

	pending  *utils.SafeMap[task]
	assigned *utils.SafeMap[task]
	finished *utils.SafeMap[task]
}

func newTaskSet(total int, tasks *utils.SafeMap[task]) *TaskSet {
	return &TaskSet{
		total:    total,
		pending:  tasks,
		assigned: utils.NewSafeMap[task](),
		finished: utils.NewSafeMap[task](),
	}
}

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

func (ts *TaskSet) SetFinished(taskId string, outputs []string) bool {
	task, ok := ts.assigned.Get(taskId)
	if !ok {
		return false
	}

	task.Outputs = outputs

	ts.assigned.Delete(taskId)
	ts.finished.Put(taskId, task)

	return true
}

func (ts *TaskSet) SetPending(taskId string) bool {
	task, ok := ts.assigned.Get(taskId)
	if !ok {
		return false
	}

	task.AssignedTime = 0
	task.AssignedWorkerId = ""

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
