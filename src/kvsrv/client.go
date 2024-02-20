package kvsrv

import (
	"crypto/rand"
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
	// You'll have to add code here.
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

	ok := ck.server.Call("KVServer.Get", &args, &reply)
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	return ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
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
			slog.Info("Retry %v for error: %v\n", n, err)
		}),
		retry.RetryIf(func(err error) bool {
			switch err := err.(type) {
			case net.Error:
				return err.Temporary()
			default:
				return false
			}
		}),
	)

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
