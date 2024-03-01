package utils

import "sync/atomic"

type Incrementer struct {
	Value int64
}

func (i *Incrementer) Increment() int64 {
	return atomic.AddInt64(&i.Value, 1)
}
