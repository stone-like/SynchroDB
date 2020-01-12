package exhash

import (
	"container/list"
	"testing"
)

type Pair struct {
	key   uint64
	value interface{}
}

func TestSplit(t *testing.T) {

	hash := NewExHash(1, 4)
	hashlist := list.New()

	for _, v := range []uint64{2, 4, 6, 8, 10} {
		pair := &Pair{
			key:   v,
			value: "",
		}

		listElem := hashlist.PushFront(pair)
		hash.Put(v, listElem)
	}

	if hash.directory.globalDepth != 2 {
		t.Errorf("actualDepth:%d,want:%d", hash.directory.globalDepth, 2)
	}

	_, ok4 := hash.directory.buckets[0].bucketMap[4]
	_, ok8 := hash.directory.buckets[0].bucketMap[8]
	_, ok2 := hash.directory.buckets[2].bucketMap[2]
	_, ok6 := hash.directory.buckets[2].bucketMap[6]
	_, ok10 := hash.directory.buckets[2].bucketMap[10]

	if !(ok4 && ok8 && ok2 && ok6 && ok10) {
		t.Errorf("bucketSplitError: ok4:%t,ok8:%t,ok2:%t,ok6:%t,ok10:%t", ok4, ok8, ok2, ok6, ok10)
	}

	if hash.directory.buckets[0].depth != 2 {
		t.Errorf("localDepthError: %d", hash.directory.buckets[0].depth)
	}

	if hash.directory.buckets[2].depth != 2 {
		t.Errorf("localDepthError: %d", hash.directory.buckets[2].depth)
	}

}

func TestPutAndGet(t *testing.T) {
	hash := NewExHash(1, 4)
	hashlist := list.New()

	for _, v := range []uint64{2, 4} {
		pair := &Pair{
			key:   v,
			value: "",
		}

		listElem := hashlist.PushFront(pair)
		hash.Put(v, listElem)
	}
	//2でgetできて、6でgetできなければいい
	_, isExist := hash.Get(uint64(2))
	if !isExist {
		t.Errorf("hashGetError2")
	}

	_, isExist = hash.Get(uint64(6))
	if isExist {
		t.Errorf("hashGetError6")

	}
}
