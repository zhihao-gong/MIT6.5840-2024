package kvsrv

import (
	"crypto/rand"
	"log"
	"log/slog"
	"math/big"

	"6.5840/labrpc"
	"6.5840/utils"
	snowflake "github.com/bwmarrin/snowflake"
)

type Clerk struct {
	id      int64
	reqIncr utils.Incrementer
	server  *labrpc.ClientEnd
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server

	node, err := snowflake.NewNode(utils.Random1024())
	if err != nil {
		log.Fatal(err)
	}

	ck.id = int64(node.Generate())
	ck.reqIncr = utils.Incrementer{Value: 0}

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
		Id: ReqId{
			Client: ck.id,
			Seq:    ck.reqIncr.Increment(),
		},
	}
	reply := GetReply{}

	// FIXME: do we need retry here?
	// ck.callWithRetry("KVServer.Get", &args, &reply, 1000000)
	ok := ck.server.Call("KVServer.Get", &args, &reply)
	if !ok {
		slog.Error("")
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op OperationType) string {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Id: ReqId{
			Client: ck.id,
			Seq:    ck.reqIncr.Increment(),
		},
	}
	reply := PutAppendReply{}

	switch op {
	case PutOps:
		ok := ck.server.Call("KVServer.Put", &args, &reply)
		if !ok {
			slog.Error("Error calling Put RPC")
		}
	case AppendOps:
		ok := ck.server.Call("KVServer.Append", &args, &reply)
		if !ok {
			slog.Error("Error calling Append RPC")
		}
	default:
		panic("Unkown operation type")
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOps)
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, AppendOps)
}
