package mr

import (
	"log/slog"
	"strconv"
	"sync"
	"time"

	"6.5840/utils"
)

// Task is the unit of work for the worker
// Captialize the first letter of the fields to make them public to rpc modules
type task struct {
	Id string

	TaskType taskType
	Inputs   []string
	Outputs  []string

	NReduce          int
	AssignedWorkerId string
	AssignedTime     int64
}

type taskManager struct {
	nReduce           int
	reassignThreshold int64

	mapTasks    *taskSet
	reduceTasks *taskSet

	phase phase
	mutex sync.RWMutex
}

type taskType int

const (
	mapTaskType taskType = iota
	reduceTaskType
)

type phase int

const (
	mapPhase phase = iota
	reducePhase
	donePhase
)

func newTaskManager(nReduce int, mapTasks *utils.SafeMap[task], reassignThreshold int64) *taskManager {
	ts := &taskManager{
		nReduce:           nReduce,
		mapTasks:          newTaskSet(int(mapTasks.Len()), mapTasks),
		reduceTasks:       nil,
		phase:             mapPhase,
		reassignThreshold: reassignThreshold,
	}
	ts.audit()

	return ts
}

func (tm *taskManager) scheduleTask(workerId string) *task {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	switch tm.phase {
	case mapPhase:
		return tm.mapTasks.getPending(workerId)
	case reducePhase:
		return tm.reduceTasks.getPending(workerId)
	default:
		return nil
	}
}

func (tm *taskManager) setFinished(taskId string, outputs []string, workerId string) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	switch tm.phase {
	case mapPhase:
		success := tm.mapTasks.setFinished(taskId, outputs, workerId)
		if success && tm.mapTasks.allFinished() {
			tm.phase = reducePhase
			tm.reduceTasks = newTaskSet(tm.nReduce, tm.initReduceTasks())
			slog.Info("Map phase finished, start reduce phase now")
		}
		return success
	case reducePhase:
		success := tm.reduceTasks.setFinished(taskId, outputs, workerId)
		if success && tm.reduceTasks.allFinished() {
			tm.phase = donePhase
			slog.Info("Reduce phase finished, all tasks done")
		}
		return success
	default:
		return false
	}
}

func (tm *taskManager) setPending(taskId string, workerId string) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	switch tm.phase {
	case mapPhase:
		return tm.mapTasks.setPending(taskId, workerId)
	case reducePhase:
		return tm.reduceTasks.setPending(taskId, workerId)
	default:
		return false
	}
}

func (tm *taskManager) initReduceTasks() *utils.SafeMap[task] {
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
			TaskType: reduceTaskType,
			Inputs:   InputFiles,
		})
	}

	return reduceTasks
}

func (tm *taskManager) audit() {
	timer := time.NewTicker(1 * time.Second)

	go func() {
		for range timer.C {
			tm.cancelTimeoutTask()
		}
	}()
}

func (tm *taskManager) cancelTimeoutTask() {
	now := time.Now().Unix()

	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	switch tm.phase {
	case mapPhase:
		for _, t := range tm.mapTasks.assigned.Values() {
			if now-t.AssignedTime >= tm.reassignThreshold {
				ok := tm.mapTasks.setPending(t.Id, "")
				if !ok {
					slog.Error("Failed to cancel map task: " + t.Id)
				}
			}
		}
	case reducePhase:
		for _, t := range tm.reduceTasks.assigned.Values() {
			if now-t.AssignedTime >= tm.reassignThreshold {
				ok := tm.reduceTasks.setPending(t.Id, "")
				if !ok {
					slog.Error("Failed to cancel reduce task: " + t.Id)
				}
			}
		}
	}
}

func (tm *taskManager) cancelTaskForWorker(workerId string) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	switch tm.phase {
	case mapPhase:
		for _, t := range tm.mapTasks.assigned.Values() {
			if t.AssignedWorkerId == workerId {
				ok := tm.mapTasks.setPending(t.Id, workerId)
				if !ok {
					slog.Error("Failed to cancel map task: " + t.Id)
				}
			}
		}
	case reducePhase:
		for _, t := range tm.reduceTasks.assigned.Values() {
			if t.AssignedWorkerId == workerId {
				ok := tm.reduceTasks.setPending(t.Id, workerId)
				if !ok {
					slog.Error("Failed to cancel reduce task: " + t.Id)
				}
			}
		}
	}
}

type taskSet struct {
	total int

	pending  *utils.SafeMap[task]
	assigned *utils.SafeMap[task]
	finished *utils.SafeMap[task]
}

func newTaskSet(total int, tasks *utils.SafeMap[task]) *taskSet {
	return &taskSet{
		total:    total,
		pending:  tasks,
		assigned: utils.NewSafeMap[task](),
		finished: utils.NewSafeMap[task](),
	}
}

func (ts *taskSet) getPending(workerId string) *task {
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

func (ts *taskSet) setFinished(taskId string, outputs []string, workerId string) bool {
	task, ok := ts.assigned.Get(taskId)
	if !ok {
		return false
	}

	if workerId != "" && workerId != task.AssignedWorkerId {
		return false
	}

	task.Outputs = outputs

	ts.assigned.Delete(taskId)
	ts.finished.Put(taskId, task)

	return true
}

func (ts *taskSet) setPending(taskId string, workerId string) bool {
	task, ok := ts.assigned.Get(taskId)
	if !ok {
		return false
	}

	if workerId != "" && workerId != task.AssignedWorkerId {
		return false
	}

	task.AssignedTime = 0
	task.AssignedWorkerId = ""

	ts.assigned.Delete(taskId)
	ts.pending.Put(taskId, task)

	return true
}

func (ts *taskSet) allFinished() bool {
	return ts.finished.Len() == int(ts.total)
}
