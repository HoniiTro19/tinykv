package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, _ := server.storage.Reader(nil)
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		log.Error("fail to get data in raw get, " + err.Error())
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}
	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	data := storage.Put{Cf: req.Cf, Key: req.Key, Value: req.Value}
	batch := []storage.Modify{
		{Data: data},
	}
	err := server.storage.Write(nil, batch)
	if err != nil {
		log.Error("fail to put data in raw put, " + err.Error())
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	data := storage.Delete{Cf: req.Cf, Key: req.Key}
	batch := []storage.Modify{
		{Data: data},
	}
	err := server.storage.Write(nil, batch)
	if err != nil {
		log.Error("fail to put data in raw put, " + err.Error())
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, _ := server.storage.Reader(nil)
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	var kvs []*kvrpcpb.KvPair
	for i := 0; i < int(req.Limit); i++ {
		if !iter.Valid() {
			log.Info("end scanning from start key: " + string(req.StartKey))
			break
		}
		key := iter.Item().Key()
		val, err := iter.Item().Value()
		if err != nil {
			log.Error("get error while scanning from start key: " + string(req.StartKey) + ", meet key: " + string(key))
			return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
		}
		kv := &kvrpcpb.KvPair{Key: key, Value: val}
		kvs = append(kvs, kv)
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
