package boltc

import (
	"fmt"
	"runtime"

	"github.com/jiuzhou-zhao/bolt-client/pkg/db"
	"github.com/jiuzhou-zhao/bolt-server/pkg/boltsc"
)

type bucketProxy struct {
	baseURL  string
	svrURL   string
	bucketID string
}

func newBucketProxy(baseURL, bucketID string) *bucketProxy {
	bucket := &bucketProxy{
		baseURL:  baseURL,
		svrURL:   makeURL(baseURL, boltsc.UriBucket),
		bucketID: bucketID,
	}
	runtime.SetFinalizer(bucket, func(oi interface{}) {
		oi.(*bucketProxy).Close()
	})
	return bucket
}

func (bucket *bucketProxy) postJSON(req interface{}) (resp *boltsc.BucketResponse, err error) {
	var respObj boltsc.BucketResponse
	err = doPostJSON(bucket.svrURL, req, &respObj)
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

func (bucket *bucketProxy) Put(key []byte, value []byte) error {
	req := &boltsc.BucketRequest{
		Method:   boltsc.BucketMethodPut,
		BucketID: bucket.bucketID,
		Key:      key,
		Value:    value,
	}
	_, err := bucket.postJSON(req)
	return err
}

func (bucket *bucketProxy) Get(key []byte) []byte {
	req := &boltsc.BucketRequest{
		Method:   boltsc.BucketMethodGet,
		BucketID: bucket.bucketID,
		Key:      key,
	}
	resp, err := bucket.postJSON(req)
	if err != nil {
		return nil
	}
	return resp.Value
}

func (bucket *bucketProxy) Delete(key []byte) error {
	req := &boltsc.BucketRequest{
		Method:   boltsc.BucketMethodDelete,
		BucketID: bucket.bucketID,
		Key:      key,
	}
	_, err := bucket.postJSON(req)
	return err
}

func (bucket *bucketProxy) Cursor() db.Cursor {
	req := &boltsc.BucketRequest{
		Method:   boltsc.BucketMethodCursor,
		BucketID: bucket.bucketID,
	}
	resp, err := bucket.postJSON(req)
	if err != nil {
		return nil
	}
	return newCursorProxy(bucket.baseURL, resp.CursorID)
}

func (bucket *bucketProxy) Close() {
	req := &boltsc.BucketRequest{
		Method:   boltsc.BucketMethodClose,
		BucketID: bucket.bucketID,
	}
	_, _ = bucket.postJSON(req)
}
