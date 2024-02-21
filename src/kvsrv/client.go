package kvsrv

import (
	"crypto/rand"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/rpc"
	"os"

	"6.5840/labrpc"
	"github.com/avast/retry-go"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
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
	}
	reply := GetReply{}

	callWithRetry("KVServer.Get", &args, &reply)

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
	}
	reply := PutAppendReply{}

	// FIXME remove retry logics for lineability
	switch op {
	case PutOps:
		callWithRetry("KVServer.Put", &args, &reply)
	case AppendOps:
		callWithRetry("KVServer.Append", &args, &reply)
	default:
		panic("Unkown operation type")
	}

	return reply.OldValue
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOps)
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, AppendOps)
}

func callWithRetry(rpcname string,
	args interface{}, reply interface{}) {

	err := retry.Do(
		func() error {
			return call(rpcname, args, reply)
		},
		retry.Attempts(3),
		retry.DelayType(retry.BackOffDelay),
		retry.OnRetry(func(n uint, err error) {
			slog.Info("Retry %v for error: %v\n", fmt.Sprint(n), err)
		}),
		retry.RetryIf(func(err error) bool {
			switch err := err.(type) {
			case net.Error:
				// FIXME: fix depreciated
				return err.Temporary()
			default:
				return false
			}
		}),
	)
	println(err)
	if err != nil {
		slog.Error("Failed to connect to coordinator, worker exiting")
		os.Exit(1)
	}
}

// send an RPC request to the coordinator, wait for the response.
// returns error if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1:8080")
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}

	return nil
}
