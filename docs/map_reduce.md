
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

## Task 分配思路

有两种任务分配机制，第一种是 worker 向 master 请求任务，
第二种是 master 向 worker 发送任务。

1. Worker 主动向 Master 申请任务：在这种机制中，Worker节点在准备好执行新任务时，会向 Master 节点发送请求以获取任务。Master 节点维护一个待分配任务的队列，并根据到来的请求将任务分配给请求的Worker。
2. Master 主动向 Worker 发送任务：在这种机制中，Master节点负责跟踪所有可用的 Worker 节点，并根据某种策略（如负载均衡、数据局部性等）主动将任务分配给 Worker。（在真实的分布式系统中，当worker机器上有部分任务的相关文件时，master 会优先分配这部分任务给这个worker）。这要求 Master 具有全局视图，并能够实时监控所有 Worker 的状态。

- Worker 主动申请：这种方式相对来说比较简单。此时，Master 仅仅是任务调度和分配中心，只需要响应 worker 的请求，并将代办任务分配给他们。此外，这种方法可以自然地处理 Worker 节点的异步和非均匀完成时间，因为，Worker 只有在准备号接受新任务时，才会请求任务。
- Master 主动发送任务：Master 需要持续跟踪每个Worker的状态，根据 worker 的状态来控制任务的分配。这就要求 worker 需要采用心跳包机制，将 worker 的加入，故障，离开发送给 master。同时，Master 中需要一个数据结构存放这些信息。

在这个 lab 中，我采用了 Worker 主动申请的机制，因为稍微方便一点。

### Task 结构体

task 结构体 coordinator 用来保存每个任务的 meta 信息, 同时会被分配给 worker;  inputs/outputs
记录任务输入和输出文件

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

### Worker 轮询申请并处理 task

worker 轮询向 coordinator 申请任务, 注意任务处理需要是原子的

对于 map 任务, 输出的是多个中间文件, 可以随机申请一个 tmp 目录, 将中间文件向 tmp 目录里写, 这样不同的 map 任务各自往不同的目录输出, 彼此不会有冲突

对于 reduce 任务, 输出的是一个文件, 通过 os.Rename 即可保证文件生成的原子性

map/reduce 的输出文件会被汇报给 coordinator

```go

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
```

### 根据当前所处阶段对应分配任务

任务阶段分为 map/reduce/exit, 每次分配任务前检测当前所处阶段, 分配对应类型的任务

如果没有当前阶段的任务, 返回 nil 给 worker, worker 啥都不做等待下一次任务轮询

```go
type phase int

const (
 mapPhase phase = iota
 reducePhase
 donePhase
)


func (tm *taskManager) scheduleTask(workerId string) *task {
 tm.mutex.Lock()
 defer tm.mutex.Unlock()

 switch tm.phase {
 case mapPhase:
  return tm.mapTasks.getPending(workerId)
 case reducePhase:
  return tm.reduceTasks.getPending(workerId)
 default:
  return &task{
   TaskType: exitTaskType,
   // value of id for exitTask does not matter
   Id: uuid.New().String(),
  }
 }
}
```

### 当前阶段所有任务完成后切换到下一阶段

当每个一个任务完成后, 检查当前阶段的任务是否全部完成, 如果是, 则切换到下一个阶段

```go

func (tm *taskManager) setFinished(taskId string, outputs []string, workerId string) bool {
 tm.mutex.Lock()
 defer tm.mutex.Unlock()

 switch tm.phase {
 case mapPhase:
  success := tm.mapTasks.setFinished(taskId, outputs, workerId)
  if success && tm.mapTasks.allFinished() {
   tm.phase = reducePhase
   tm.reduceTasks = newTaskSet(tm.nReduce, tm.initReduceTasks())
   slog.Info("Map phase finished, start reduce phase now")
  }
  return success
 case reducePhase:
  success := tm.reduceTasks.setFinished(taskId, outputs, workerId)
  if success && tm.reduceTasks.allFinished() {
   tm.phase = donePhase
   slog.Info("Reduce phase finished, all tasks done")
  }
  return success
 default:
  return false
 }
}

```

### 超时任务取消

在分配任务时候将时间点记录于 task 结构体中, 启动一个后台的 goroutine 阶段性检查所有 task 中的对应时间戳是否超过阈值,
如果是, 则将任务状态改回成 pending

```go

func (tm *taskManager) audit() {
 timer := time.NewTicker(1 * time.Second)

 go func() {
  for range timer.C {
   tm.cancelTimeoutTask()
  }
 }()
}

func (tm *taskManager) cancelTimeoutTask() {
 now := time.Now().Unix()

 tm.mutex.Lock()
 defer tm.mutex.Unlock()

 switch tm.phase {
 case mapPhase:
  for _, t := range tm.mapTasks.assigned.Values() {
   if now-t.AssignedTime >= tm.reassignThreshold {
    ok := tm.mapTasks.setPending(t.Id, "")
    if !ok {
     slog.Error("Failed to cancel map task: " + t.Id)
    }
   }
  }
 case reducePhase:
  for _, t := range tm.reduceTasks.assigned.Values() {
   if now-t.AssignedTime >= tm.reassignThreshold {
    ok := tm.reduceTasks.setPending(t.Id, "")
    if !ok {
     slog.Error("Failed to cancel reduce task: " + t.Id)
    }
   }
  }
 }
}
```

### Worker 汇报任务结果

Worker 在处理完 map/reduce 任务后通过 reportTaskExecution 汇报任务结果, 如果结果成功, 任务被放到 finished 队列 , 如果结果失败, 任务被放回 pending 队列, 后续有 worker 申请任务时候再做分配

```go

// ReportTaskExecution is called by worker to report the status of the task execution
func (c *Coordinator) ReportTaskExecution(args *ReportTaskExecutionArgs, reply *ReportTaskExecutionReply) error {
 ...
 if args.ExecuteSuccess {
  slog.Info("Task finished: " + args.TaskId)
  ok = c.taskManager.setFinished(args.TaskId, args.Outputs, args.WorkerId)
 } else {
  slog.Info("Task execution failed and reverted to pending queue: " + args.TaskId)
  ok = c.taskManager.setPending(args.TaskId, args.WorkerId)
 }

 if !ok {
  reply.Result.Code = 1
  reply.Result.Message = "Task not found"
  slog.Error("Task not found: " + args.TaskId)
  return nil
 }

 reply.Result.Code = 0
 reply.Result.Message = "Reported"

 return nil
}
```

## 其他提高项

根据论文还有下面这些功能可以做, 本次 lab 没有实现

1. master 容错方案论文给出的是定期 checkpoint 做持久化, 没有更多实现细节, 我的想法是
   1. 通过 map/reduce 服务外的平台, 例如: k8s, 监控 master 状态, master 挂了之后做 failover
   2. 部署多个 master 节点, master 节点之间通过共识算法同步状态, leader 节点失联后选举新的 leader
2. 任务备份解决短板任务慢的问题
3. 基于 map/reduce 任务是确定的, 本地调试可以复现远程场景
4. 将任务分配到数据源所在节点, 减少网络传输
5. 抽象数据源, 支持从 db 或者其他格式数据源
