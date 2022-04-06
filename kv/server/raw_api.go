package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	resp := new(kvrpcpb.RawGetResponse)
	v, err := r.GetCF(req.Cf, req.Key)
	if v == nil {
		resp.NotFound = true
	} else {
		resp.Value = v
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	p := storage.Put {
		Key: req.Key,
		Value: req.Value,
		Cf: req.Cf,
	}
	m := storage.Modify{
		Data: p,
	}
	batch := [] storage.Modify{m}
	err := server.storage.Write(nil, batch)
	resp := new(kvrpcpb.RawPutResponse)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	p := storage.Delete {
		Key: req.Key,
		Cf: req.Cf,
	}
	m := storage.Modify{
		Data: p,
	}
	err := server.storage.Write(nil, [] storage.Modify{m})
	resp := new(kvrpcpb.RawDeleteResponse)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	r, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	resp := new(kvrpcpb.RawScanResponse)
	it := r.IterCF(req.Cf)
	it.Seek(req.StartKey)
	for i := 0; i < int(req.Limit); i++ {
		if !it.Valid() {
			break
		}
		item := it.Item()
		val, err := item.Value()
		if err != nil {
			continue
		}
		p := kvrpcpb.KvPair {
			Key: item.Key(),
			Value: val,
		}
		resp.Kvs = append(resp.Kvs, &p)
		it.Next()
	}
	return resp, nil
}
