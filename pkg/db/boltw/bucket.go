package boltw

import (
	"github.com/boltdb/bolt"
	"github.com/jiuzhou-zhao/bolt-client/pkg/db"
)

type bucketImpl struct {
	bucket *bolt.Bucket
}

func newBucket(bucket *bolt.Bucket) db.Bucket {
	if bucket == nil {
		return nil
	}
	return &bucketImpl{bucket: bucket}
}

func (impl *bucketImpl) Put(key []byte, value []byte) error {
	return impl.bucket.Put(key, value)
}
func (impl *bucketImpl) Get(key []byte) []byte {
	return impl.bucket.Get(key)
}

func (impl *bucketImpl) Delete(key []byte) error {
	return impl.bucket.Delete(key)
}

func (impl *bucketImpl) Cursor() db.Cursor {
	return newCursor(impl.bucket.Cursor())
}
