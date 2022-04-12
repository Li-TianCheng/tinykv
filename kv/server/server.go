package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()

	keysToLatch := [][]byte{req.Key}
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, _ := txn.GetLock(req.Key)
	if lock != nil && lock.Ts <= req.Version {
		resp.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(req.Key),
		}
		return resp, nil
	}
	value, _ := txn.GetValue(req.Key)
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	if len(req.Mutations) == 0 {
		return resp, nil
	}
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()

	var keysToLatch [][]byte
	for _, m := range req.Mutations {
		keysToLatch = append(keysToLatch, m.Key)
	}
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	keyErrors := checkPrewriteConflict(txn, req)
	if keyErrors != nil {
		resp.Errors = keyErrors
		return resp, nil
	}
	for _, m := range req.Mutations {
		lock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
		}
		switch m.Op {
		case kvrpcpb.Op_Put:
			lock.Kind = mvcc.WriteKindPut
			txn.PutValue(m.Key, m.Value)
		case kvrpcpb.Op_Del:
			lock.Kind = mvcc.WriteKindDelete
			txn.DeleteLock(m.Key)
		}
		txn.PutLock(m.Key, lock)
	}
	server.storage.Write(req.Context, txn.Writes())
	return resp, nil
}

func checkPrewriteConflict(txn *mvcc.MvccTxn, req *kvrpcpb.PrewriteRequest) []*kvrpcpb.KeyError {
	var keyErrors []*kvrpcpb.KeyError
	for _, m := range req.Mutations {
		write, ts, _ := txn.MostRecentWrite(m.Key)
		if write != nil && ts >= txn.StartTS {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Key:        m.Key,
					Primary:    req.PrimaryLock,
				},
			})
		} else {
			lock, _ := txn.GetLock(m.Key)
			if lock != nil && lock.Ts != txn.StartTS {
				keyErrors = append(keyErrors, &kvrpcpb.KeyError{
					Locked: lock.Info(m.Key),
				})
			}
		}
	}
	return keyErrors
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	//resp := &kvrpcpb.PrewriteResponse{}
	resp := &kvrpcpb.CommitResponse{}
	if len(req.Keys) == 0 {
		return resp, nil
	}
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()

	var keysToLatch [][]byte
	for _, key := range req.Keys {
		keysToLatch = append(keysToLatch, key)
	}
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	keyError := checkCommitConflict(txn, req)
	if keyError != nil {
		resp.Error = keyError
		return resp, nil
	}
	for _, key := range req.Keys {
		lock, _ := txn.GetLock(key)
		if lock != nil {
			write := &mvcc.Write{
				StartTS: lock.Ts,
				Kind:    lock.Kind,
			}
			txn.PutWrite(key, req.CommitVersion, write)
			txn.DeleteLock(key)
		}
	}
	server.storage.Write(req.Context, txn.Writes())
	return resp, nil
}

func checkCommitConflict(txn *mvcc.MvccTxn, req *kvrpcpb.CommitRequest) *kvrpcpb.KeyError {
	for _, key := range req.Keys {
		lock, _ := txn.GetLock(key)
		if lock == nil {
			write, tx, _ := txn.CurrentWrite(key)
			if write == nil || tx != req.CommitVersion || write.Kind == mvcc.WriteKindRollback {
				return &kvrpcpb.KeyError{}
			}
		}
		if lock != nil && lock.Ts != req.StartVersion {
			return &kvrpcpb.KeyError{}
		}
	}
	return nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)

	scan := mvcc.NewScanner(req.StartKey, txn)
	var keys [][]byte
	var values [][]byte
	for i := 0; uint32(i) < req.Limit; i++ {
		key, value, _ := scan.Next()
		if key == nil {
			break
		}
		if value != nil {
			keys = append(keys, key)
			values = append(values, value)
		}
	}
	scan.Close()
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	for i, key := range keys {
		lock, _ := txn.GetLock(key)
		pair := &kvrpcpb.KvPair{
			Key: key,
		}
		if lock != nil && lock.Ts <= req.Version {
			pair.Error = &kvrpcpb.KeyError{
				Locked: lock.Info(key),
			}
		} else {
			pair.Value = values[i]
		}
		resp.Pairs = append(resp.Pairs, pair)
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()

	keysToLatch := [][]byte{req.PrimaryKey}
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, ts, _ := txn.CurrentWrite(req.PrimaryKey)
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = ts
		return resp, nil
	}
	lock, _ := txn.GetLock(req.PrimaryKey)
	write = &mvcc.Write{
		StartTS: req.LockTs,
		Kind:    mvcc.WriteKindRollback,
	}
	if lock == nil {
		txn.PutWrite(req.PrimaryKey, req.LockTs, write)
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	} else {
		resp.LockTtl = lock.Ttl
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
			txn.DeleteLock(req.PrimaryKey)
			txn.DeleteValue(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, req.LockTs, write)
			resp.LockTtl = 0
			resp.Action = kvrpcpb.Action_TTLExpireRollback
		}
	}
	server.storage.Write(req.Context, txn.Writes())
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		write, _, _ := txn.CurrentWrite(key)
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				resp.Error = &kvrpcpb.KeyError{
					Abort: "true",
				}
				return resp, nil
			}
		}
		lock, _ := txn.GetLock(key)
		if lock != nil && lock.Ts == req.StartVersion {
			txn.DeleteLock(key)
		}
		txn.DeleteValue(key)
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	server.storage.Write(req.Context, txn.Writes())
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	if req.StartVersion == 0 {
		return resp, nil
	}
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()

	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()

	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		value, _ := item.Value()
		lock, _ := mvcc.ParseLock(value)
		if lock.Ts == req.StartVersion {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return resp, nil
	}

	if req.CommitVersion == 0 {
		res, _ := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		resp.Error = res.Error
	} else {
		res, _ := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		resp.Error = res.Error
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
