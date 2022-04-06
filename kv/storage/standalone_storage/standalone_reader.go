package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/Connor1996/badger"
)

type StandAloneStorageReader struct {
	txn *badger.Txn
	ctx *kvrpcpb.Context
}

func NewStandAloneStorageReader(s *StandAloneStorage, ctx *kvrpcpb.Context) (*StandAloneStorageReader, error) {
	r := StandAloneStorageReader {
		txn: s.db.NewTransaction(true),
		ctx: ctx,
	}
	return &r, nil
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}
