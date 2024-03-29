package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"6.5840/utils"
	"github.com/avast/retry-go"
)

// Worker is the interface for the worker
type myWorker struct {
	workerId   string
	status     workerStatus
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string

	pendingTasks utils.SafeQueue[*task]
}

// Register worker on the corrdinator side and get the assigned id
func (w *myWorker) register() string {
	args := RegisterArgs{}
	reply := RegisterReply{}

	callWithRetry("Coordinator.Register", &args, &reply)
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

	callWithRetry("Coordinator.Ping", &args, &reply)
	if reply.Result.Code != 0 {
		slog.Error("Ping error: " + reply.Result.Message)
		return
	}
}

// Ask the coordinator for a task
func (w *myWorker) askForTask() *task {
	args := AskForTaskArgs{
		WorkerId: w.workerId,
	}

	reply := AskForTaskReply{}

	callWithRetry("Coordinator.AskForTask", &args, &reply)
	if reply.result.Code != 0 {
		slog.Error("AskForTask error: " + reply.result.Message)
		return nil
	}

	return &reply.Task
}

// Loop forever to ask and work on the job assigned from coordinator
func (w *myWorker) doJob() {
	for {
		pending := w.pendingTasks.Dequeue()

		if pending == nil {
			newTask := w.askForTask()
			// notes: if id is empty, it means no task assigned by coordinator
			if newTask == nil || (*newTask).Id == "" {
				time.Sleep(1 * time.Second)
			} else {
				w.pendingTasks.Enqueue(newTask)
			}
			continue
		}

		switch (*pending).TaskType {
		case mapTaskType:
			slog.Info("Handling map task: " + (*pending).Id)
			outputs, err := w.handleMapTask(*pending)
			if err != nil {
				slog.Info(err.Error())
				w.reportTaskExecution((*pending).Id, false, outputs)
			} else {
				w.reportTaskExecution((*pending).Id, true, outputs)
			}
		case reduceTaskType:
			slog.Info("Handling reduce task: " + (*pending).Id)
			outputs, err := w.handleReduceTask(*pending)
			if err != nil {
				slog.Info(err.Error())
				w.reportTaskExecution((*pending).Id, false, outputs)
			} else {
				w.reportTaskExecution((*pending).Id, true, outputs)
			}
		case exitTaskType:
			slog.Info("Going to exit the program")
			os.Exit(0)
		}
	}
}

// handle map task:
// read inputs, call mapfunc, write output
func (w *myWorker) handleMapTask(task *task) ([]string, error) {
	if len((*task).Inputs) != 1 {
		panic("Map task should have only one input file")
	}

	inputFile := (*task).Inputs[0]
	content, err := utils.ReadFile(inputFile)
	if err != nil {
		return nil, err
	}

	kva := w.mapFunc(inputFile, content)

	nReduce := (*task).NReduce
	intermediate := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		intermediate[i] = make([]KeyValue, 0)
	}
	for _, kv := range kva {
		partition := ihash(kv.Key) % nReduce
		intermediate[partition] = append(intermediate[partition], kv)
	}

	results := make([]string, 0)
	for i := 0; i < nReduce; i++ {
		dir, err := os.MkdirTemp("", "mr-tmp-*")
		if err != nil {
			return nil, err
		}
		fileName := fmt.Sprintf("mr-%s-%d", (*task).Id, i)
		outputFile := filepath.Join(dir, fileName)

		data, err := json.MarshalIndent(intermediate[i], "", " ")
		if err != nil {
			return nil, err
		}

		err = utils.WriteFile(outputFile, data)
		if err != nil {
			return nil, err
		}

		results = append(results, outputFile)
	}

	return results, nil
}

// handle reduce task
func (w *myWorker) handleReduceTask(task *task) ([]string, error) {

	// read all intermediate files
	intermediate := make([]KeyValue, 0)
	for _, inputFile := range (*task).Inputs {
		content, err := utils.ReadFile(inputFile)
		if err != nil {
			return nil, err
		}

		var data []KeyValue
		err = json.Unmarshal([]byte(content), &data)
		if err != nil {
			return nil, err
		}
		intermediate = append(intermediate, data...)
	}

	sort.Sort(byKey(intermediate))

	// group by key and call reduce func
	i := 0
	outputs := make([]string, 0)
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		ret := w.reduceFunc(intermediate[i].Key, values)

		line := fmt.Sprintf("%v %v\n", intermediate[i].Key, ret)
		outputs = append(outputs, line)
		i = j
	}

	outputFile := fmt.Sprintf("mr-out-%s", (*task).Id)
	err := utils.WriteFile(outputFile, []byte(strings.Join(outputs, "")))
	if err != nil {
		return nil, err
	}

	return outputs, nil
}

// Report task execution status to the coordinator
func (w *myWorker) reportTaskExecution(taskId string, success bool, outputs []string) {
	args := ReportTaskExecutionArgs{
		WorkerId:       w.workerId,
		TaskId:         taskId,
		ExecuteSuccess: success,
		Outputs:        outputs,
	}
	reply := ReportTaskExecutionReply{}

	callWithRetry("Coordinator.ReportTaskExecution", &args, &reply)
	if reply.Result.Code != 0 {
		slog.Error("ReportTaskExecution error: " + reply.Result.Message)
		return
	}
}

// Start the worker
func (w *myWorker) start() {
	w.workerId = w.register()
	slog.Info("Registered successfully with assigned id: " + w.workerId)

	timer := time.NewTicker(10 * time.Second)
	go func() {
		for range timer.C {
			w.ping()
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
		status:     normal,
		mapFunc:    mapf,
		reduceFunc: reducef,
	}
	worker.start()
}

func callWithRetry(rpcname string,
	args interface{}, reply interface{}) {

	err := retry.Do(
		func() error {
			return call(rpcname, args, reply)
		},
		retry.Attempts(3),
		retry.DelayType(retry.BackOffDelay),
		retry.OnRetry(func(n uint, err error) {
			slog.Info("Retry %v for error: %v\n", n, err)
		}),
		retry.RetryIf(func(err error) bool {
			switch err := err.(type) {
			case net.Error:
				return err.Temporary()
			default:
				return false
			}
		}),
	)

	if err != nil {
		slog.Error("Failed to connect to coordinator, worker exiting")
		os.Exit(1)
	}
}

// send an RPC request to the coordinator, wait for the response.
// returns error if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1:8080")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}

	return nil
}
