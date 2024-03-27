package kvsrv

import "log"

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
	Value string
}

type GetArgs struct {
	Key string
	Id  ReqId
}

type GetReply struct {
	Value string
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
