package mr

import (
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	"6.5840/utils"
	"github.com/google/uuid"
)

// Coordinator holds all the information about the current state of the map reduce job
type Coordinator struct {
	workers *WorkerSet

	mapTasks    *TaskSet
	reduceTasks *TaskSet

	currPhase Phase

	keepAliveTheshold int64
}

// RegisterWorker generates a unique ID for a new worker, assigns it to the worker and returns the ID
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	assignedId := uuid.New().String()
	c.workers.mapping.Put(assignedId, worker{
		id:           assignedId,
		lastPingTime: time.Now().Unix(),
		status:       Idle,
	})

	reply.result.Code = 0
	reply.WorkerId = assignedId
	reply.result.Message = "Registered"

	return nil
}

// AskForTask is called by worker for a task if appropriate
func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	_, ok := c.workers.mapping.Get(args.WorkerId)
	if !ok {
		reply.result.Code = 1
		reply.result.Message = "Worker not found"
		return nil
	}

	reply.Task = *c.scheduleTask(args.WorkerId)

	return nil
}

// Ping is called by worker to report the status of the worker(keep alive)
func (c *Coordinator) Ping(args *PingArgs, reply *PingReply) error {
	c.workers.mutex.Lock()
	defer c.workers.mutex.Unlock()

	worker, ok := c.workers.mapping.Get(args.WorkerId)
	if !ok {
		reply.Result.Code = 1
		reply.Result.Message = "Worker not found"
		slog.Error("Worker not found: " + args.WorkerId)
		return nil
	}

	slog.Info("Ping from worker:" + args.WorkerId)

	worker.lastPingTime = time.Now().Unix()
	if worker.status == Lost {
		worker.status = Idle
	}
	c.workers.mapping.Put(args.WorkerId, worker)

	reply.Result.Code = 0
	reply.Result.Message = "Reported"

	return nil
}

// Schedule a task to a worker
func (c *Coordinator) scheduleTask(workerId string) *task {
	if c.currPhase == MapPhaseType {
		return c.mapTasks.GetPendingTask(workerId)
	} else {
		return c.reduceTasks.GetPendingTask(workerId)
	}
}

// Check if a worker is lost of connection, reassigned the task if appropriate
func (c *Coordinator) auditWorkerStatus() {
	c.workers.mutex.Lock()
	defer c.workers.mutex.Unlock()

	for _, worker := range c.workers.mapping.Values() {
		if time.Now().Unix()-worker.lastPingTime > c.keepAliveTheshold {
			worker.status = Lost
			c.workers.mapping.Put(worker.id, worker)
		}
	}
}

// Start the coordinator
func (c *Coordinator) start() {
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

// create map tasks based on input files
func initMapTasks(files []string, nReduce int) *utils.SafeMap[task] {
	mapTasks := utils.NewSafeMap[task]()

	for _, file := range files {
		id := uuid.New().String()
		InputFiles := make([]string, 1)
		InputFiles[0] = file

		mapTasks.Put(id, task{
			id:       id,
			taskType: MapTaskType,
			inputs:   InputFiles,
		})
	}

	return mapTasks
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := initMapTasks(files, nReduce)
	c := Coordinator{
		workers:           NewWorkerSet(),
		mapTasks:          NewTaskSet(int64(mapTasks.Len()), mapTasks),
		keepAliveTheshold: 60,
	}

	c.start()
	return &c
}
