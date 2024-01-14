package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"6.5840/utils"
	"github.com/google/uuid"
)

// Coordinator holds all the information about the current state of the map reduce job
type Coordinator struct {
	workers *utils.SafeMap[worker]

	mapTasks    *TaskSet
	reduceTasks *TaskSet

	currPhase Phase

	keepAliveTheshold int64
}

// RegisterWorker generates a unique ID for a new worker, assigns it to the worker and returns the ID
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	assignedId := uuid.New().String()

	c.workers.Put(assignedId, worker{
		id:           assignedId,
		lastPingTime: time.Now().Unix(),
		status:       Idle,
	})

	reply.result.Code = 0
	reply.WorkerId = assignedId
	reply.result.Message = "Registered"

	return nil
}

// AskForTask is called by worker to report the status of the worker(keep alive) and assign a task if appropriate
func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	worker, ok := c.workers.Get(args.WorkerId)
	if !ok {
		reply.result.Code = 1
		reply.result.Message = "Worker not found"
		return nil
	}

	worker.lastPingTime = time.Now().Unix()
	c.workers.Put(args.WorkerId, worker)

	reply.result.Code = 0
	reply.result.Message = "Reported"

	reply.Task = *c.scheduleTask(args.WorkerId)

	return nil
}

// Schedule a task to a worker
func (c *Coordinator) scheduleTask(workerId string) *Task {
	if c.currPhase == MapPhaseType {
		return c.mapTasks.Get(workerId)
	} else {
		return c.reduceTasks.Get(workerId)
	}
}

// Check if a worker is lost of connection, reassigned the task if appropriate
func (c *Coordinator) auditWorkerStatus() {
	for _, worker := range c.workers.Values() {
		if time.Now().Unix()-worker.lastPingTime > c.keepAliveTheshold {
			worker.status = Lost
			c.workers.Put(worker.id, worker)
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

// create tasks based on input files
func createTasks(files []string, nReduce int) (*utils.SafeMap[Task], *utils.SafeMap[Task]) {
	mapTasks := utils.NewSafeMap[Task]()

	for _, file := range files {
		id := uuid.New().String()
		InputFiles := make([]string, 1)
		InputFiles[0] = file

		outputFiles := make([]string, nReduce)
		for i := 0; i < nReduce; i++ {
			outputFiles[i] = filepath.Join(os.TempDir(), "mr-"+id+"-"+strconv.Itoa(i))
		}
		mapTasks.Put(id, Task{
			Id:          id,
			Type:        MapTaskType,
			InputFiles:  InputFiles,
			OutputFiles: outputFiles,
		})
	}

	reduceTasks := utils.NewSafeMap[Task]()
	mapTasksCopy := mapTasks.Copy()
	for i := 0; i < nReduce; i++ {
		id := uuid.New().String()

		j := 0
		InputFiles := make([]string, len(mapTasksCopy))
		for _, t := range mapTasksCopy {
			InputFiles[j] = t.OutputFiles[i]
			j++
		}

		outputFiles := []string{filepath.Join(os.TempDir(), "mr-out-"+strconv.Itoa(i))}
		reduceTasks.Put(id, Task{
			Id:          id,
			Type:        ReduceTaskType,
			InputFiles:  InputFiles,
			OutputFiles: outputFiles,
		})
	}

	return mapTasks, reduceTasks
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks, reduceTasks := createTasks(files, nReduce)
	c := Coordinator{
		workers:           utils.NewSafeMap[worker](),
		mapTasks:          NewTaskSet(int64(mapTasks.Len()), mapTasks),
		reduceTasks:       NewTaskSet(int64(reduceTasks.Len()), reduceTasks),
		keepAliveTheshold: 60,
	}

	c.start()
	return &c
}
