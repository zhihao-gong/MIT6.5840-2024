package mr

type Scheduler interface {
	Schedule(task Task)
}
