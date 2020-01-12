package exhash

import (
	"container/list"
)

type ExHash struct {
	// depth      uint32
	// bucketSize uint32
	directory *Directory
}

//最初はglobalDepth絶対１にしなきゃいけないと思ってたけど別にそうでもないのかな？
//ここら辺がよくわからず元ネタでは一応globaldepthは1になってるっぽいけど
func NewExHash(depth, bucketSize uint64) *ExHash {
	directory := NewDirectory(depth, bucketSize)
	return &ExHash{
		directory: directory,
	}
}

//directoryを操作する,keyとDataを引数にkeyはinterface{}なんだけどDataは*list.Elementなんだよね・・・
//exHashのgetとかでは*list.Elementを返せばよい
//この段階でもうHashkeyを渡してしまうか？それともKey構造体を作ってそこに役割を押し付けるか、Keyにおいての比較がどのように使われるか
func (eh *ExHash) Get(key uint64) (*list.Element, bool) {
	return eh.directory.Search(key)
}
func (eh *ExHash) Put(key uint64, listElem *list.Element) {
	eh.directory.Insert(key, listElem,false)
}
func (eh *ExHash) Update(key uint64, listElem *list.Element) {
	eh.directory.Update(key, listElem)
}
func (eh *ExHash) Delete(key uint64) {
	eh.directory.Remove(key)
}

// func (eh *ExHash) CountDir()                                      {}
