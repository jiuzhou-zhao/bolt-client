package boltc

import (
	"fmt"
	"runtime"

	"github.com/jiuzhou-zhao/bolt-server/pkg/boltsc"
)

type cursorProxy struct {
	baseURL  string
	svrURL   string
	cursorID string
}

func newCursorProxy(baseURL, cursorID string) *cursorProxy {
	cursor := &cursorProxy{
		baseURL:  baseURL,
		svrURL:   makeURL(baseURL, boltsc.UriCursor),
		cursorID: cursorID,
	}
	runtime.SetFinalizer(cursor, func(oi interface{}) {
		oi.(*cursorProxy).Close()
	})
	return cursor
}

func (cursor *cursorProxy) postJSON(req interface{}) (resp *boltsc.CursorResponse, err error) {
	var respObj boltsc.CursorResponse
	err = doPostJSON(cursor.svrURL, req, &respObj)
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

func (cursor *cursorProxy) First() (key []byte, value []byte) {
	req := &boltsc.CursorRequest{
		Method:   boltsc.CursorMethodFirst,
		CursorID: cursor.cursorID,
	}
	resp, err := cursor.postJSON(req)
	if err != nil {
		return nil, nil
	}
	return resp.Key, resp.Value
}

func (cursor *cursorProxy) Next() (key []byte, value []byte) {
	req := &boltsc.CursorRequest{
		Method:   boltsc.CursorMethodNext,
		CursorID: cursor.cursorID,
	}
	resp, err := cursor.postJSON(req)
	if err != nil {
		return nil, nil
	}
	return resp.Key, resp.Value
}

func (cursor *cursorProxy) Close() {
	req := &boltsc.CursorRequest{
		Method:   boltsc.CursorMethodClose,
		CursorID: cursor.cursorID,
	}
	_, _ = cursor.postJSON(req)
}
