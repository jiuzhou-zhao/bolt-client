package boltw

import (
	"os"

	"github.com/boltdb/bolt"
	"github.com/jiuzhou-zhao/bolt-client/pkg/db"
)

type dbImpl struct {
	path string
	dB   *bolt.DB
}

func NewDB(path string) (db.DB, error) {
	dB, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &dbImpl{
		path: path,
		dB:   dB,
	}, nil
}

func (impl *dbImpl) Close() error {
	return impl.dB.Close()
}

func (impl *dbImpl) Update(fn func(db.Tx) error) error {
	return impl.dB.Update(func(tx *bolt.Tx) error {
		return fn(newTx(tx))
	})
}
func (impl *dbImpl) View(fn func(db.Tx) error) error {
	return impl.dB.View(func(tx *bolt.Tx) error {
		return fn(newTx(tx))
	})
}

func (impl *dbImpl) Destroy() error {
	return os.Remove(impl.path)
}
