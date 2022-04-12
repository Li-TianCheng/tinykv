package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn  *MvccTxn
	iter engine_util.DBIterator
	lastKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		txn: txn,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
		lastKey: startKey,
	}
	scanner.iter.Seek(EncodeKey(startKey, scanner.txn.StartTS))
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for scan.iter.Valid(){
		item := scan.iter.Item()
		curr := item.Key()
		if curr != nil {
			key := DecodeUserKey(curr)
			ts := decodeTimestamp(curr)
			if ts < scan.txn.StartTS && !bytes.Equal(scan.lastKey, key){
				value, _ := scan.txn.GetValue(key)
				scan.lastKey = key
				return key, value, nil
			}
		}
		scan.iter.Next()
	}
	return nil, nil, nil
}
