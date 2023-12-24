package mr

import "sync"

type SafeMap struct {
	mu sync.RWMutex
	m  map[interface{}]interface{}
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		m: make(map[interface{}]interface{}),
	}
}

func (sm *SafeMap) Put(key, value interface{}) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[key] = value
}

func (sm *SafeMap) Get(key interface{}) (interface{}, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	value, ok := sm.m[key]
	return value, ok
}

func (sm *SafeMap) Delete(key interface{}) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.m, key)
}
