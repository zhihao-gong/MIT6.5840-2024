package utils

import (
	"sync"
)

type SafeQueue[T any] struct {
	queue []T
	mutex sync.Mutex
}

func (q *SafeQueue[T]) Enqueue(t T) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.queue = append(q.queue, t)
}

func (q *SafeQueue[T]) Dequeue() *T {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.queue) == 0 {
		return nil
	}

	task := q.queue[0]
	q.queue = q.queue[1:]

	return &task
}
