package mr

import (
	"log/slog"
	"strconv"
	"sync"
	"time"

	"6.5840/utils"
)

type TaskManager struct {
	nReduce     int
	mapTasks    *TaskSet
	reduceTasks *TaskSet

	phase Phase
	mutex sync.RWMutex
}

type Phase int

const (
	MapPhaseType Phase = iota
	ReducePhaseType
	DonePhaseType
)

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

func (tm *TaskManager) setFinished(taskId string, outputs []string, workerId string) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if tm.phase == MapPhaseType {
		success := tm.mapTasks.SetFinished(taskId, outputs, workerId)
		if success && tm.mapTasks.AllFinished() {
			tm.phase = ReducePhaseType
			tm.reduceTasks = newTaskSet(tm.nReduce, tm.initReduceTasks())
			slog.Info("Map phase finished, start reduce phase now")
		}
		return success
	} else if tm.phase == ReducePhaseType {
		success := tm.reduceTasks.SetFinished(taskId, outputs, workerId)
		if success && tm.reduceTasks.AllFinished() {
			tm.phase = DonePhaseType
			slog.Info("Reduce phase finished, all tasks done")
		}
		return success
	} else {
		return false
	}
}

func (tm *TaskManager) setPending(taskId string, workerId string) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if tm.phase == MapPhaseType {
		return tm.mapTasks.SetPending(taskId, workerId)
	} else if tm.phase == ReducePhaseType {
		return tm.reduceTasks.SetPending(taskId, workerId)
	} else {
		return false
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

func (ts *TaskSet) SetFinished(taskId string, outputs []string, workerId string) bool {
	task, ok := ts.assigned.Get(taskId)
	if !ok {
		return false
	}

	if task.AssignedWorkerId != workerId {
		return false
	}

	task.Outputs = outputs

	ts.assigned.Delete(taskId)
	ts.finished.Put(taskId, task)

	return true
}

func (ts *TaskSet) SetPending(taskId string, workerId string) bool {
	task, ok := ts.assigned.Get(taskId)
	if !ok {
		return false
	}

	if task.AssignedWorkerId != workerId {
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
