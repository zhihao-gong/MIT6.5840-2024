package kvsrv

type OperationType int

const (
	PutOps OperationType = iota
	AppendOps
)

type ReqId struct {
	Client int64
	Seq    int64
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Id    ReqId
}

type PutAppendReply struct {
	OldValue string
}

type GetArgs struct {
	Key string
	Id  ReqId
}

type GetReply struct {
	Value string
}
