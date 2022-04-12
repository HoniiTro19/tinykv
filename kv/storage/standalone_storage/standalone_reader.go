package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
)

// StandaloneReader in an implementation of 'StorageReader'
type StandAloneReader struct {
	Db *badger.DB
}

func NewStandAloneReader(db *badger.DB) *StandAloneReader {
	return &StandAloneReader{
		Db: db,
	}
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	// Your Data Here (1).
	val, err := engine_util.GetCF(s.Db, cf, key)
	if err != nil {
		log.Error("fail to get value from column family key, " + err.Error())
		if err.Error() == "Key not found" {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	// Your Data Here (1).
	iter := engine_util.NewCFIterator(cf, s.Db.NewTransaction(false))
	return iter
}

func (s *StandAloneReader) Close() {
	// Your Data Here (1).
	s.Db.Close()
}
