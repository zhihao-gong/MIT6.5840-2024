package kvsrv

import (
	"log"

	"6.5840/utils"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DedupReq[T any, Y any](operation func(t *T, y *Y)) func(t *T, y *Y) {
	return func(t *T, y *Y) {
		operation(t, y)
	}
}

type KVServer struct {
	store       utils.ConcurrentMap[string, string]
	deduplicate utils.ConcurrentMap[string, uint] // key: client id, value: request sequence number
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	key := args.Key
	reply.Value, _ = kv.store.Get(key)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	key := args.Key
	value := args.Value

	shard := kv.store.GetShard(args.Key)
	shard.Lock()
	defer shard.Unlock()
	oldValue, exists := shard.Items[key]
	shard.Items[key] = value

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

	oldValue, exists := shard.Items[key]
	if !exists {
		shard.Items[key] = value
		reply.OldValue = ""
	} else {
		shard.Items[key] = oldValue + value
		reply.OldValue = oldValue
	}

}

func StartKVServer() *KVServer {
	kv := &KVServer{
		store: utils.New[string](),
	}

	return kv
}
