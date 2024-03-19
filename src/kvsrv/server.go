package kvsrv

import (
	"6.5840/utils"
)

type KVServer struct {
	store    utils.ConcurrentMap[string, string]
	reqTable utils.ConcurrentMap[int64, record] // key: client id, value: request sequence number
}

type record struct {
	seq   int64
	value string
}

func DedupRequest(
	reqTable *utils.ConcurrentMap[int64, record],
	clientId int64,
	reqSeq int64,
	fn func() string) {

	shard := reqTable.GetShard(clientId)
	shard.Lock()
	defer shard.Unlock()

	oldRecord, exists := shard.Items[clientId]
	if !exists {
		// value := fn()
		shard.Items[clientId] = record{seq: reqSeq, value: ""}
	}

	if reqSeq == oldRecord.seq {
		return
	} else if reqSeq > oldRecord.seq {
		shard.Items[clientId] = record{seq: reqSeq}
	} else {
		panic("Request sequence number is less than the one in the record")
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DedupRequest(&kv.reqTable, args.Id.Client, args.Id.Seq, func() string {
		key := args.Key
		reply.Value, _ = kv.store.Get(key)
		return reply.Value
	})
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	DedupRequest(&kv.reqTable, args.Id.Client, args.Id.Seq, func() string {
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

		return reply.OldValue
	})
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	DedupRequest(&kv.reqTable, args.Id.Client, args.Id.Seq, func() string {
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

		return reply.OldValue
	})
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		store: utils.New[string](),
	}

	return kv
}
