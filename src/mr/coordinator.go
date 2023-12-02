package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type worker struct {
	Id           string
	LastPingTime int64
	status       WorkerStatus
}

type MapTask struct {
	Id       string
	FileName string
}

type ReduceTask struct {
	Id string
}

// Coordinator holds all the information about the current state of the map reduce job
type Coordinator struct {
	WorkerMutex sync.RWMutex
	Workers     map[string]worker

	MapTaskMutex sync.RWMutex
	MapTasks     map[string]MapTask

	ReduceTaskMutex sync.RWMutex
	ReduceTasks     map[string]ReduceTask

	KeepAliveTheshold int64
}

// RegisterWorker generates a unique ID for a new worker, assigns it to the worker and returns the ID
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	assignedId := uuid.New().String()

	c.WorkerMutex.Lock()
	c.Workers[assignedId] = worker{
		Id:           assignedId,
		LastPingTime: time.Now().Unix(),
		status:       Idle,
	}
	c.WorkerMutex.Unlock()

	reply.Code = 0
	reply.AssgnedId = assignedId
	reply.Message = "Registered"

	return nil
}

// Ping is called by the worker to let the coordinator know that it is still alive
// func (c *Coordinator) Ping(args *PingArgs, reply *PingReply) error {
// 	workerId := args.AssignedId

// 	c.WorkerMutex.Lock()
// 	defer c.WorkerMutex.Unlock()

// 	worker, ok := c.Workers[workerId]
// 	if !ok {
// 		return errors.New("worker not found for id: " + workerId)
// 	}

// 	worker.LastPingTime = time.Now().Unix()
// 	c.Workers[workerId] = worker

// 	return nil
// }

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		KeepAliveTheshold: 10,
		Workers:           make(map[string]worker),
		WorkerMutex:       sync.RWMutex{},
	}

	c.server()
	return &c
}
