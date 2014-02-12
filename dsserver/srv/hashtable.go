package srv

import (
	"github.com/HouzuoGuo/tiedot/dsserver/dstruct"
)

type HashOpen struct {
	Path, IndexName string
}

type HashReq struct {
	IndexName string
	Key, Val, Limit    uint64
}

type HashResp struct {
	Keys, Vals []uint64
}

func (srv *RpcServer) Hopen(in *HashOpen, _ *bool) error {
	ht, err := dstruct.OpenHash(in.Path, []string{})
	if err != nil {
		return err
	}
	srv.Htables[in.IndexName] = ht
	return nil
}

func (srv *RpcServer) Hset(in *HashReq, _ *bool) error {
	srv.Htables[in.IndexName].Put(in.Key, in.Val)
	return nil
}

func (srv *RpcServer) Hget(in *HashReq, out *HashResp) error {
	out.Keys, out.Vals = srv.Htables[in.IndexName].Get(in.Key, in.Limit)
	return nil
}

func (srv *RpcServer) Hdel(in *HashReq, _ *bool) error {
	srv.Htables[in.IndexName].Remove(in.Key, in.Val)
	return nil
}
