package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"log/slog"
	"net/rpc"
	"os"
	"time"

	"6.5840/utils"
)

// Worker is the interface for the worker
type myWorker struct {
	workerId   string
	status     WorkerStatus
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string

	pendingTasks utils.SafeQueue[*Task]
}

// Register worker on the corrdinator side and get the assigned id
func (w *myWorker) register() string {
	args := RegisterArgs{}
	reply := RegisterReply{}

	ok := call("Coordinator.Register", &args, &reply)

	// TODO: Add retry logics
	if !ok {
		slog.Error("Register error while rpc")
		os.Exit(1)
	}
	if reply.result.Code != 0 {
		slog.Error("Register error: " + reply.result.Message)
		os.Exit(1)
	}

	return reply.WorkerId
}

// Ping the coordinator to report the aliveness of the worker
func (w *myWorker) ping() {
	args := PingArgs{
		WorkerId: w.workerId,
	}
	reply := PingReply{}

	ok := call("Coordinator.Ping", &args, &reply)
	if !ok {
		slog.Error("Ping error while rpc")
		return
	}
	if reply.Result.Code != 0 {
		slog.Error("Ping error: " + reply.Result.Message)
		return
	}
}

// Ask the coordinator for a task
func (w *myWorker) askForTask() *Task {
	args := AskForTaskArgs{
		WorkerId: w.workerId,
	}

	reply := AskForTaskReply{}

	ok := call("Coordinator.AskForTask", &args, &reply)
	if !ok {
		slog.Error("AskForTask error while rpc")
		return nil
	}

	if reply.result.Code != 0 {
		slog.Error("AskForTask error: " + reply.result.Message)
		return nil
	}

	return &reply.Task
}

// Loop forever to ask and work on the job assigned from coordinator
func (w *myWorker) doJob() {
	for {
		task := w.pendingTasks.Dequeue()
		if task == nil {
			newTask := w.askForTask()
			if newTask == nil {
				time.Sleep(1 * time.Second)
			} else {
				w.pendingTasks.Enqueue(newTask)
			}
			continue
		}

		switch (*task).Type {
		case MapTaskType:
			if len((*task).InputFiles) != 1 {
				panic("Map task should have only one input file")
			}
			inputFile := (*task).InputFiles[0]
			// FIXME: Handle filenotfound exception
			content, _ := utils.ReadFile(inputFile)
			kva := w.mapFunc(inputFile, content)
		case ReduceTaskType:
			// w.reduceFunc(task.Input, content)

		}
	}
}

// Report finished task to the coordinator
func (w *myWorker) ReportFinishedTask(taskId string) {
	args := ReportFinishedTaskArgs{
		WorkerId: w.workerId,
		TaskId:   taskId,
	}
	reply := ReportFinishedTaskReply{}

	// TODO: Add retry logics
	ok := call("Coordinator.ReportFinishedTask", &args, &reply)
	if !ok {
		slog.Error("ReportFinishedTask error while rpc")
		return
	}
	if reply.result.Code != 0 {
		slog.Error("ReportFinishedTask error: " + reply.result.Message)
		return
	}
}

// Start the worker and keep reporting the status to the coordinator
func (w *myWorker) Start() {
	w.workerId = w.register()
	slog.Info("Registered successfully with assigned id: " + w.workerId)

	timer := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-timer.C:
				w.ping()
			}
		}
	}()

	go w.doJob()

	// Block forever
	select {}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := myWorker{
		status:     Idle,
		mapFunc:    mapf,
		reduceFunc: reducef,
	}
	worker.Start()
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
