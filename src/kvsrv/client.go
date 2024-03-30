package kvsrv

import (
	"crypto/rand"
	"fmt"
	"log"
	"log/slog"
	"math/big"
	"os"

	"6.5840/labrpc"
	"6.5840/utils"
	"github.com/avast/retry-go"
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

	ck.callWithRetry("KVServer.Get", &args, &reply, 1000000)

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
		ck.callWithRetry("KVServer.Put", &args, &reply, 10000)
	case AppendOps:
		ck.callWithRetry("KVServer.Append", &args, &reply, 1000)
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

func (ck *Clerk) callWithRetry(rpcname string,
	args interface{}, reply interface{}, attempts uint) {
	err := retry.Do(
		func() error {
			ok := ck.server.Call(rpcname, args, reply)
			if !ok {
				return fmt.Errorf("error calling %s RPC", rpcname)
			}
			return nil
		},
		retry.Attempts(attempts),
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
