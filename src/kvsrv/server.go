package kvsrv

import (
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"

	cmap "github.com/orcaman/concurrent-map/v2"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	store *cmap.ConcurrentMap[string, string]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	key := args.Key
	reply.Value, _ = kv.store.Get(key)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) error {
	key := args.Key
	value := args.Value

	shard := kv.store.GetShard(args.Key)
	shard.Lock()
	defer shard.Unlock()
	oldValue, exists := shard.items[key]
	shard.items[key] = value

	if !exists {
		reply.OldValue = ""
	} else {
		reply.OldValue = oldValue
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	key := args.Key
	value := args.Value

	shard := kv.store.GetShard(args.Key)

	shard.Lock()
	defer shard.Unlock()

	oldValue, exists := shard.items[key]
	if exists {
		shard.items[key] = value
		reply.OldValue = ""
	} else {
		shard.items[key] = oldValue + value
		reply.OldValue = oldValue
	}
}

// start a thread that listens for RPCs from worker.go
func (kv *KVServer) server() {
	rpc.Register(kv)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8080")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.server()

	slog.Info("Keyserver started")

	return kv
}
