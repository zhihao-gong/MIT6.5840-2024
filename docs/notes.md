# map reduce

## 设计动机记录

1. Coordinator 不会主动向 worker 发 rpc 请求, 因为不希望多个 worker 时候, 每个 worker 都暴露不同端口

2. Task 结构不添加 status 字段, 而是通过不同的队列, 例如: pendingQueue, assignedQueue 来管理不同的任务, 这样避免了每次都要遍历队列搜索指定状态的任务

3. Coordinate 通过 worker 的行为(比如是否按期 ping)统一判断 worker 状态, 而不是由 worker 汇报状态

## TODO

- [ ] 增加一个 notes 来记录设计和可能的改进点
- [ ] Debug crashed worker case failed
