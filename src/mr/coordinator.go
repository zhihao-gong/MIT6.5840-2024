package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

type worker struct {
	Id           string
	LastPingTime int64
	status       WorkerStatus
}

type Phase int

const (
	Map = iota
	Reduce
)

// Coordinator holds all the information about the current state of the map reduce job
type Coordinator struct {
	WorkerMutex sync.RWMutex
	Workers     map[string]worker

	TaskMutex sync.RWMutex
	Tasks     map[string]Task

	CurrPhase Phase

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
	reply.WorkerId = assignedId
	reply.Message = "Registered"

	return nil
}

// AskForTask is called by worker to report the status of the worker(keep alive) and assign a task if appropriate
func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	c.WorkerMutex.Lock()
	defer c.WorkerMutex.Unlock()

	worker, ok := c.Workers[args.WorkerId]
	if !ok {
		reply.Code = 1
		reply.Message = "Worker not found"
		return nil
	}

	worker.LastPingTime = time.Now().Unix()
	worker.status = args.Status
	c.Workers[args.WorkerId] = worker

	reply.Code = 0
	reply.Message = "Reported"

	// TODO: Assign task

	return nil
}

// Check if a worker is lost of connection, reassigned the task if appropriate
func (c *Coordinator) auditWorkerStatus() {
	c.WorkerMutex.Lock()
	defer c.WorkerMutex.Unlock()

	for _, worker := range c.Workers {
		if time.Now().Unix()-worker.LastPingTime > c.KeepAliveTheshold {
			// Update the status of the worker to lost
			worker.status = Lost
		}
	}
}

// Start the coordinator
func (c *Coordinator) Start() {
	timer := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case <-timer.C:
				c.auditWorkerStatus()
			}
		}
	}()

	c.server()
}

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

// create tasks based on input files
func createTasks(files []string, nReduce int) map[string]*Task {
	tasks := make(map[string]*Task)

	for _, file := range files {
		id := uuid.New().String()
		input := make([]filename, len(file))
		for i, f := range file {
			input[i] = filename(f)
		}

		outputFiles := make([]filename, nReduce)
		for i := 0; i < nReduce; i++ {
			outputFiles[i] = filepath.Join(os.TempDir(), "mr-"+id+"-"+strconv.Itoa(i))
		}
		tasks[id] = &Task{
			Id:     file,
			Type:   Map,
			Input:  input,
			Output: outputFiles,
		}
	}

	return tasks
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		KeepAliveTheshold: 60,
		Workers:           make(map[string]worker),
		WorkerMutex:       sync.RWMutex{},
	}

	c.Start()
	return &c
}
