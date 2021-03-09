package boltc

import (
	"log"
	"runtime"
	"testing"
	"time"
)

type TestObj struct {
	Name string
}

func (obj *TestObj) Close() {
	log.Printf("%s finalizer\n", obj.Name)
}

func TestFinalizer(t *testing.T) {
	fn := func() {
		obj1 := new(TestObj)
		obj1.Name = "obj1"
		runtime.SetFinalizer(obj1, func(o interface{}) {
			o.(*TestObj).Close()
		})
		//runtime.SetFinalizer(obj1, func(i interface{}) {
		//	println("垃圾回收了")
		//})
		t.Log(obj1)
		obj1 = nil
	}
	fn()
	runtime.GC()
	time.Sleep(time.Second)
}
