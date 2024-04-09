
# map reduce 实现

## rpc 状态码

对于 rpc 请求的返回状态, 通过 RpcResult 这个结构表示, Code 枚举可能的状态, Message 给出可读的提示信息

```go

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

```

在每个 reply 结构中带上 RpcResult, 例如:

```go

type RegisterReply struct {
    ...
    result   RpcResult
}
```

## Worker 注册

Worker 在启动后向 coordinator 发 rpc 请求, coordinator 生成一个 uuid 作为 workerid 返回给 worker

优化项: 可以采用 twitter 开源的雪花算法来取代 uuid, 雪花算法可以用 int64 表示 id, 大量减少了存储空间

其他实现: worker id 的生成也可以让 worker 来做, worker 把生成好的 worker id 注册给 coordinator, 前提是保证每个 worker 生成的 id 是唯一

## 任务分配

有两种实现思路

1. Worker 主动发请求向 coordinator 申请任务
2. Coordinator 主动发请求向 worker 分配任务

如果用第二种实现,

### Task 结构体

task 结构体 coordinator 用来保存每个任务的 meta 信息, 同时会被分配给 worker, worker 根据结构题里的 inputs/outputs
路径读取输入, 做计算, 并生成输出文件

注意: map/reduce 模型中 map 任务输入是一个文件, 输出是一组文件, reduce 任务输入是一组文件, 输出是一个文件,

这里 task 结构体中的 Inputs/Outputs 统一用数组表示, 不再针对 map, reduce 任务的输入输出做更细的区分, worker 在处理的时候根据 taskType 是 map 还是 reduce 对应处理即可

```go

type taskType int

const (
 mapTaskType taskType = iota
 reduceTaskType
 // tell worker to exit as all tasks have been finished
 exitTaskType
)

// Task is the unit of work for the worker
// Captialize the first letter of the fields to make them public to rpc modules
type task struct {
 Id string

 TaskType taskType
 Inputs   []string
 Outputs  []string

 NReduce int

 // worker info will be set when assigned
 AssignedWorkerId string
 AssignedTime     int64
}

```
