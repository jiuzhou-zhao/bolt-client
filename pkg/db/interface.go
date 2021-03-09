package db

type DB interface {
	Update(fn func(Tx) error) error
	View(fn func(Tx) error) error
	Close() error
	Destroy() error
}

type Tx interface {
	CreateBucket(name []byte) (Bucket, error)
	Bucket(name []byte) Bucket
	DeleteBucket(name []byte) error
}

type Bucket interface {
	Put(key []byte, value []byte) error
	Get(key []byte) []byte
	Delete(key []byte) error
	Cursor() Cursor
}

type Cursor interface {
	First() (key []byte, value []byte)
	Next() (key []byte, value []byte)
}
