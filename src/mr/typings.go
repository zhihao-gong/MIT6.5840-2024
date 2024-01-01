package mr

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
