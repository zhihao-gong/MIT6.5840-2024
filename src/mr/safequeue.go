package mr

import (
	"sync"
)

type SafeQueue struct {
	queue []Task
	mutex sync.Mutex
}

func (q *SafeQueue) Enqueue(t Task) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.queue = append(q.queue, t)
}

func (q *SafeQueue) Dequeue() *Task {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.queue) == 0 {
		return nil
	}

	task := q.queue[0]
	q.queue = q.queue[1:]

	return &task
}
