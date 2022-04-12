package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	Engines *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	path := conf.DBPath
	israft := conf.Raft
	kvPath := path + "/kv/"
	dbKv := engine_util.CreateDB(kvPath, false)
	var engine *engine_util.Engines
	if israft {
		raftPath := path + "/raft/"
		dbRaft := engine_util.CreateDB(raftPath, true)
		engine = engine_util.NewEngines(dbKv, dbRaft, kvPath, raftPath)
	} else {
		engine = engine_util.NewEngines(dbKv, nil, kvPath, "")
	}

	return &StandAloneStorage{
		Engines: engine,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.Engines.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := NewStandAloneReader(s.Engines.Kv)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writes := new(engine_util.WriteBatch)
	for _, modify := range batch {
		if len(modify.Value()) == 0 {
			log.Info("get delete operation, cf: " + string(modify.Cf()) + ", key:" + string(modify.Key()))
			writes.DeleteCF(modify.Cf(), modify.Key())
		} else {
			log.Info("get put operation, cf: " + string(modify.Cf()) + ", key:" + string(modify.Key()) + ", value:" + string(modify.Value()))
			writes.SetCF(modify.Cf(), modify.Key(), modify.Value())
		}
	}
	err := s.Engines.WriteKV(writes)
	if err != nil {
		log.Error("fail to write batch data into key value database, " + err.Error())
		return err
	}
	log.Info("success to write batch data into key value database")
	return nil
}
