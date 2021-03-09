package boltc

import (
	"fmt"

	"github.com/jiuzhou-zhao/bolt-client/pkg/db"
	"github.com/jiuzhou-zhao/bolt-server/pkg/boltsc"
)

type tXProxy struct {
	baseURL string
	svrURL  string
	txID    string
}

func newTxProxy(baseURL, txID string) *tXProxy {
	tX := &tXProxy{
		baseURL: baseURL,
		svrURL:  makeURL(baseURL, boltsc.UriTX),
		txID:    txID,
	}
	return tX
}

func (tX *tXProxy) postJSON(req interface{}) (resp *boltsc.TXResponse, err error) {
	var respObj boltsc.TXResponse
	err = doPostJSON(tX.svrURL, req, &respObj)
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

func (tX *tXProxy) CreateBucket(name []byte) (db.Bucket, error) {
	req := &boltsc.TXRequest{
		Method:     boltsc.TXMethodCreateBucket,
		TxID:       tX.txID,
		BucketName: string(name),
	}
	resp, err := tX.postJSON(req)
	if err != nil {
		return nil, err
	}
	return newBucketProxy(tX.baseURL, resp.BucketID), nil
}
func (tX *tXProxy) Bucket(name []byte) db.Bucket {
	req := &boltsc.TXRequest{
		Method:     boltsc.TXMethodGetBucket,
		TxID:       tX.txID,
		BucketName: string(name),
	}
	resp, err := tX.postJSON(req)
	if err != nil {
		return nil
	}
	return newBucketProxy(tX.baseURL, resp.BucketID)
}
func (tX *tXProxy) DeleteBucket(name []byte) error {
	req := &boltsc.TXRequest{
		Method:     boltsc.TXMethodDeleteBucket,
		TxID:       tX.txID,
		BucketName: string(name),
	}
	_, err := tX.postJSON(req)
	return err
}

func (tX *tXProxy) Commit() error {
	req := &boltsc.TXRequest{
		Method: boltsc.TXMethodClose,
		TxID:   tX.txID,
	}
	_, err := tX.postJSON(req)
	return err
}

func (tX *tXProxy) Rollback(err error) error {
	var rollbackMessage string
	if err != nil {
		rollbackMessage = err.Error()
	}
	if rollbackMessage == "" {
		rollbackMessage = "rollback"
	}
	req := &boltsc.TXRequest{
		Method:          boltsc.TXMethodClose,
		TxID:            tX.txID,
		RollbackMessage: rollbackMessage,
	}
	_, err = tX.postJSON(req)
	return err
}
