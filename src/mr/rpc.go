package mr

//
// RPC definitions.
//

type RpcStatusCode int

const (
	Success RpcStatusCode = iota
	Error
	NotFound
	Unauthorized
	Forbidden
)

type RpcResult struct {
	Code    RpcStatusCode
	Message string
}

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId string
	result   RpcResult
}

type PingArgs struct {
	WorkerId string
}

type PingReply struct {
	Result RpcResult
}

type AskForTaskArgs struct {
	WorkerId string
}

type AskForTaskReply struct {
	Task   task
	result RpcResult
}

type ReportTaskExecutionArgs struct {
	WorkerId       string
	TaskId         string
	ExecuteSuccess bool
	Outputs        []string
}

type ReportTaskExecutionReply struct {
	Result RpcResult
}
