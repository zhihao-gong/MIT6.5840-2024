package mr

import (
	"sync"

	"6.5840/utils"
)

type WorkerStatus int

const (
	Normal WorkerStatus = iota
	Lost
)

type worker struct {
	id           string
	lastPingTime int64
	status       WorkerStatus
}

type WorkerSet struct {
	mutex   sync.RWMutex
	mapping *utils.SafeMap[worker]
}

func NewWorkerSet() *WorkerSet {
	return &WorkerSet{
		mapping: utils.NewSafeMap[worker](),
	}
}
