package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneReader struct {
	storage *StandAloneStorage
	txns    []*badger.Txn
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	result, err := engine_util.GetCF(r.storage.backend, cf, key)
	if err != nil && err.Error() == storage.NotFoundError {
		return nil, nil
	} else {
		return result, err
	}
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	txn := r.storage.backend.NewTransaction(false)
	r.txns = append(r.txns, txn)
	return engine_util.NewCFIterator(cf, txn)
}

func (r *StandAloneReader) Close() {
	// discard all txns
	for _, txn := range r.txns {
		txn.Discard()
	}
}
