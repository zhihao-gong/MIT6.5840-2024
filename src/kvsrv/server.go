package kvsrv

import (
	"6.5840/utils"
)

type KVServer struct {
	store utils.ConcurrentMap[string, string]
	// memorize the request seq sent by client
	// key: client id, value: record of seq and exec result
	reqTable utils.ConcurrentMap[int64, record]
}

// record is used store the current outstanding request sequence
// and the corresponding result
type record struct {
	seq   int64
	value string
}

type execMeta struct {
	// whether the request needs to be executed
	doExc bool
	// result of the old request, only valid if doExc is false
	oldResult string
	// result of execution, only valid if doExc is true
	result string
}

func DedupRequest(
	reqTable *utils.ConcurrentMap[int64, record],
	clientId int64,
	reqSeq int64,
	fn func(*execMeta)) {

	shard := reqTable.GetShard(clientId)
	shard.Lock()
	defer shard.Unlock()

	oldRecord, exists := shard.Items[clientId]
	if !exists {
		meta := execMeta{doExc: true, result: ""}
		fn(&meta)
		shard.Items[clientId] = record{seq: reqSeq, value: meta.result}
	} else if reqSeq == oldRecord.seq {
		meta := execMeta{doExc: false, oldResult: oldRecord.value}
		fn(&meta)
	} else {
		meta := execMeta{doExc: true, result: ""}
		fn(&meta)
		shard.Items[clientId] = record{seq: reqSeq, value: meta.result}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DedupRequest(&kv.reqTable, args.Id.Client, args.Id.Seq, func(meta *execMeta) {
		if !meta.doExc {
			reply.Value = meta.oldResult
			return
		}

		key := args.Key
		reply.Value, _ = kv.store.Get(key)
		meta.result = reply.Value
	})
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	DedupRequest(&kv.reqTable, args.Id.Client, args.Id.Seq, func(meta *execMeta) {
		if !meta.doExc {
			return
		}
		key := args.Key
		value := args.Value

		shard := kv.store.GetShard(args.Key)
		shard.Lock()
		defer shard.Unlock()

		shard.Items[key] = value
	})
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	DedupRequest(&kv.reqTable, args.Id.Client, args.Id.Seq, func(meta *execMeta) {
		if !meta.doExc {
			reply.Value = meta.oldResult
			return
		}
		key := args.Key
		value := args.Value

		shard := kv.store.GetShard(args.Key)

		shard.Lock()
		defer shard.Unlock()

		oldValue, exists := shard.Items[key]
		if !exists {
			shard.Items[key] = value
			reply.Value = ""
		} else {
			shard.Items[key] = oldValue + value
			reply.Value = oldValue
		}

		meta.result = reply.Value
	})
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		store:    utils.New[string](),
		reqTable: utils.NewWithCustomShardingFunction[int64, record](utils.Fnv32Int64),
	}

	return kv
}
