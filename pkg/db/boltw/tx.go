package boltw

import (
	"github.com/boltdb/bolt"
	"github.com/jiuzhou-zhao/bolt-client/pkg/db"
)

type txImpl struct {
	tx *bolt.Tx
}

func newTx(tx *bolt.Tx) db.Tx {
	if tx == nil {
		return nil
	}
	return &txImpl{tx: tx}
}

func (impl *txImpl) CreateBucket(name []byte) (db.Bucket, error) {
	bucket, err := impl.tx.CreateBucket(name)
	if err != nil {
		return nil, err
	}
	return newBucket(bucket), nil
}

func (impl *txImpl) Bucket(name []byte) db.Bucket {
	return newBucket(impl.tx.Bucket(name))
}

func (impl *txImpl) DeleteBucket(name []byte) error {
	return impl.tx.DeleteBucket(name)
}
