package boltc

import (
	"fmt"
	"runtime"

	"github.com/jiuzhou-zhao/bolt-client/pkg/db"
	"github.com/jiuzhou-zhao/bolt-server/pkg/boltsc"
)

type dBProxy struct {
	baseURL string
	svrURL  string
	dbName  string
	dbID    string
}

func DBRebuild4Debug(baseURL, dbName string) error {
	cli := &dBProxy{
		baseURL: baseURL,
		svrURL:  makeURL(baseURL, boltsc.UriDB),
		dbName:  dbName,
	}

	req := &boltsc.DBRequest{
		Method: boltsc.DBMethodRebuild4Debug,
		DBName: dbName,
	}
	_, err := cli.postJSON(req)
	return err
}

func NewDBClient(baseURL, dbName string) (db.DB, error) {
	cli := &dBProxy{
		baseURL: baseURL,
		svrURL:  makeURL(baseURL, boltsc.UriDB),
		dbName:  dbName,
	}
	err := cli.init(dbName)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func (db *dBProxy) init(dbName string) error {
	req := &boltsc.DBRequest{
		Method: boltsc.DBMethodOpen,
		DBName: dbName,
	}
	resp, err := db.postJSON(req)
	if err != nil {
		return err
	}
	db.dbID = resp.DbID
	runtime.SetFinalizer(db, func(oi interface{}) {
		_ = oi.(*dBProxy).Close()
	})
	return nil
}

func (db *dBProxy) postJSON(req interface{}) (resp *boltsc.DBResponse, err error) {
	var respObj boltsc.DBResponse
	err = doPostJSON(db.svrURL, req, &respObj)
	if err != nil {
		return
	}
	if respObj.Status != 0 {
		err = fmt.Errorf("resp failed: %d, %s", respObj.Status, respObj.Message)
		return
	}
	resp = &respObj
	return
}

func (db *dBProxy) Update(fn func(db.Tx) error) error {
	req := boltsc.DBRequest{
		Method: boltsc.DBMethodUpdate,
		DbID:   db.dbID,
	}
	resp, err := db.postJSON(req)
	if err != nil {
		return err
	}
	tX := newTxProxy(db.baseURL, resp.TxID)
	err = fn(tX)
	if err != nil {
		return tX.Rollback(err)
	}
	return tX.Commit()
}

func (db *dBProxy) View(fn func(db.Tx) error) error {
	req := boltsc.DBRequest{
		Method: boltsc.DBMethodView,
		DbID:   db.dbID,
	}
	resp, err := db.postJSON(req)
	if err != nil {
		return err
	}
	tX := newTxProxy(db.baseURL, resp.TxID)
	err = fn(tX)
	if err != nil {
		return tX.Rollback(err)
	}
	return tX.Commit()
}

func (db *dBProxy) Close() error {
	req := boltsc.DBRequest{
		Method: boltsc.DBMethodClose,
		DbID:   db.dbID,
	}
	_, err := db.postJSON(req)
	return err
}

func (db *dBProxy) Destroy() error {
	req := boltsc.DBRequest{
		Method: boltsc.DBMethodRebuild4Debug,
		DBName: db.dbName,
	}
	_, err := db.postJSON(req)
	return err
}
