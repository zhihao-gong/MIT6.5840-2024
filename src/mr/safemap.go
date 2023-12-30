package mr

import "sync"

type SafeMap[T any] struct {
	mu sync.RWMutex
	m  map[string]T
}

func NewSafeMap[T any]() *SafeMap[T] {
	return &SafeMap[T]{
		m: make(map[string]T),
	}
}

func (sm *SafeMap[T]) Put(key string, value T) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[key] = value
}

func (sm *SafeMap[T]) Get(key string) (T, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	value, ok := sm.m[key]
	return value, ok
}

func (sm *SafeMap[T]) Delete(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.m, key)
}

func (sm *SafeMap[T]) Values() []T {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	values := make([]T, 0, len(sm.m))
	for _, value := range sm.m {
		values = append(values, value)
	}
	return values
}

func (sm *SafeMap[T]) Len() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.m)
}

func (sm *SafeMap[T]) Copy() map[string]T {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	m := make(map[string]T)
	for k, v := range sm.m {
		m[k] = v
	}
	return m
}
