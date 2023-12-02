package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

type WorkerStatus int

const (
	Idle WorkerStatus = iota
	InProgress
	Completed
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type myworker struct {
	AssignedId string
	status     WorkerStatus
}

func (w *myworker) Report(args *ReportArgs, reply *ReportReply) error {
	return nil
}

func (w *myworker) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	return nil
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

	assignedId := Register()

	worker := myworker{
		AssignedId: assignedId,
		status:     Idle,
	}
	log.Println("Worker registered with id: ", worker.AssignedId)
}

// Register worker on the corrdinator side and get the assigned id
func Register() string {
	args := RegisterArgs{}
	reply := RegisterReply{}

	ok := call("Coordinator.Register", &args, &reply)

	if !ok {
		log.Fatal("Register error")
	}
	if reply.Code != 0 {
		log.Fatal("Register error: ", reply.Message)
	}

	return reply.AssgnedId
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
