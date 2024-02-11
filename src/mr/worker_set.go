package mr

import (
	"sync"

	"6.5840/utils"
)

type workerStatus int

const (
	normal workerStatus = iota
	lost
)

type worker struct {
	id           string
	lastPingTime int64
	status       workerStatus
}

type workerSet struct {
	mutex   sync.RWMutex
	mapping *utils.SafeMap[worker]
}

func newWorkerSet() *workerSet {
	return &workerSet{
		mapping: utils.NewSafeMap[worker](),
	}
}
