package kvsrv

type OperationType int

const (
	PutOps OperationType = iota
	AppendOps
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
}

type PutAppendReply struct {
	OldValue string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}
