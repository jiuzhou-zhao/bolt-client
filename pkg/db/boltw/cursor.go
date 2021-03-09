package boltw

import (
	"github.com/boltdb/bolt"
	"github.com/jiuzhou-zhao/bolt-client/pkg/db"
)

type cursorImpl struct {
	cursor *bolt.Cursor
}

func newCursor(cursor *bolt.Cursor) db.Cursor {
	if cursor == nil {
		return nil
	}
	return &cursorImpl{cursor: cursor}
}

func (impl *cursorImpl) First() (key []byte, value []byte) {
	return impl.cursor.First()
}
func (impl *cursorImpl) Next() (key []byte, value []byte) {
	return impl.cursor.Next()
}
