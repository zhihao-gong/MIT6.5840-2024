
# 实现

## client

### 重试请求

测试中 unreliable test 这个 case 会随机让请求超时以及延迟, Get/Put/Append 请求都要需要无限做重试

下面的实现了 `callWithRetry` 这个高阶函数, 将要真正要调用的函数传入即可实现重试

```go

func (ck *Clerk) Get(key string) string {
 ...

 ck.callWithRetry("KVServer.Get", &args, &reply)

 ...
}

func (ck *Clerk) PutAppend(key string, value string, op OperationType) string {
 ...

 switch op {
 case PutOps:
  ck.callWithRetry("KVServer.Put", &args, &reply)
 case AppendOps:
  ck.callWithRetry("KVServer.Append", &args, &reply)
 default:
  panic("Unkown operation type")
 }

 ...
}

func (ck *Clerk) callWithRetry(rpcname string,
 args interface{}, reply interface{}) {
 err := retry.Do(
  func() error {
   ok := ck.server.Call(rpcname, args, reply)
   if !ok {
    return fmt.Errorf("error calling %s RPC", rpcname)
   }
   return nil
  },
  retry.Attempts(10000000),
  retry.DelayType(retry.BackOffDelay),
  retry.OnRetry(func(n uint, err error) {
   DPrintf("Retry %v for error: %v\n", fmt.Sprint(n), err)
  }),
 )
 if err != nil {
  slog.Error(err.Error())
  os.Exit(1)
 }
}
```

### Cliend id 以及 request id 生成

每个 clerk 结构对应一个 client id 和一个递增器用来生成 request id

```go
type Clerk struct {
 id      int64
 reqIncr utils.Incrementer
 server  *labrpc.ClientEnd
}
```

client id 的生成采用 twitter 开源的 [雪花算法](https://zh.wikipedia.org/wiki/%E9%9B%AA%E8%8A%B1%E7%AE%97%E6%B3%95), 可以通过 int64 来存储 id, 相比于 uuid 节省了大量空间

```go

 node, err := snowflake.NewNode(utils.Random1024())
 if err != nil {
  log.Fatal(err)
 }
 ck.id = int64(node.Generate())

```

request id 通过递增器生成, 递增其用 atomic value 来实现, 相比于 mutex 更适合并发场景

```go

type Incrementer struct {
 Value int64
}

func (i *Incrementer) Increment() int64 {
 return atomic.AddInt64(&i.Value, 1)
}

```

在发 rpc 请求前调用递增器配置 request id

```go

args := GetArgs{
  Key: key,
  Id: ReqId{
   Client: ck.id,
   Seq:    ck.reqIncr.Increment(),
  },
 }

```

## server

### 存储

KVServer 里的 store 用来做存储, store 是个根据开源 [ConcurrentMap](https://github.com/orcaman/concurrent-map) 适配后的结构, 该实现对把 key 分片到不同的 map 上, 减少 mutex 带来的竞争, 更适合并发

```go
type KVServer struct {
  store utils.ConcurrentMap[string, string]
  ...
}
```

在使用 concurrent map 的时候可以直接调用 Set/Get 等 api, 对于需要对多个操作实现原子性场景, 也可以手动对分片加锁

```go
key := args.Key
value := args.Value

// 通过 key 获取分片
shard := kv.store.GetShard(args.Key)

// 手动对分片加锁
shard.Lock()
defer shard.Unlock()

oldValue, exists := shard.Items[key]
if !exists {
  shard.Items[key] = value
  reply.Value = ""
} else {
  shard.Items[key] = oldValue + value
  reply.Value = oldValue
}
```
<!--
### 去重

```go
// record is used store the current outstanding request sequence
// and the corresponding result
type record struct {
 seq   int64
 value string
}

type execMeta struct {
 // whether the request needs to be executed
 doExc bool
 // result of the old request, only valid if doExc is false
 oldResult string
 // result of execution, only valid if doExc is true
 result string
}

func DedupRequest(
 reqTable *utils.ConcurrentMap[int64, record],
 clientId int64,
 reqSeq int64,
 fn func(*execMeta)) {

 shard := reqTable.GetShard(clientId)
 shard.Lock()
 defer shard.Unlock()

 oldRecord, exists := shard.Items[clientId]
 if !exists {
  meta := execMeta{doExc: true, result: ""}
  fn(&meta)
  shard.Items[clientId] = record{seq: reqSeq, value: meta.result}
 } else if reqSeq == oldRecord.seq {
  meta := execMeta{doExc: false, oldResult: oldRecord.value}
  fn(&meta)
 } else {
  meta := execMeta{doExc: true, result: ""}
  fn(&meta)
  shard.Items[clientId] = record{seq: reqSeq, value: meta.result}
 }
}
``` -->
