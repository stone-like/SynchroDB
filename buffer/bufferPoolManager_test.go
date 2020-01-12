package buffer

import (
	"testing"

	p "github.com/stonelike/synchroDB/disk/page"
	tup "github.com/stonelike/synchroDB/disk/tuple"
)

const (
	dirName = `C:\Users\user\Desktop\DB`
	// tableName = `tempTable`
)

// func TestVictimToDisk(t *testing.T) {
// 	bufferMg := NewBufferPoolManager(1)
// 	//capを1にしたので二つinsertしたらvictimが一つ目になってほしい
// 	page := p.NewPage()
// 	bufferMg.InsertPage(bufferMg.maxPid, tableName, page)

// 	//ここで一番目に作ったpageにtupleを入れて、pagedirtyにしたい,一つしかpageがないからfreepageでとってくるのも一番目なはず
// 	var tupleData = []interface{}{"go", "tuple"}
// 	tuple := tup.NewTuple(tupleData)
// 	bufferMg.InsertTuple(tableName, tuple)
// 	//tupleを入れるときにinsertPageが呼ばれている→getFreePDが機能していない

// 	page = p.NewPage()
// 	bufferMg.InsertPage(bufferMg.maxPid, tableName, page)
// 	//これでvictimとなった一番目がdiskに書かれているはず
// 	readpage, err := bufferMg.diskManager.ReadDisk(dirName, tableName, 0)

// 	if err != nil {
// 		t.Errorf("diskManagerError:%s", err)
// 	}

// 	if readpage.Tuples[0].Data[0].String_ != "go" {
// 		t.Errorf("accutual: %s,want:%s", page.Tuples[0].Data[0].String_, "go")
// 	}

// }

// func TestDeleteFromFreeMap(t *testing.T) {
// 	bufferMg := NewBufferPoolManager(1)
// 	//capを1にしたので二つinsertしたらvictimが一つ目になってほしい
// 	page := p.NewPage()
// 	bufferMg.InsertPage(bufferMg.maxPid, tableName, page)

// 	//ここで一番目に作ったpageにtupleを入れて、pagedirtyにしたい,一つしかpageがないからfreepageでとってくるのも一番目なはず
// 	var tupleData = []interface{}{1, "tuple"}
// 	tuple := tup.NewTuple(tupleData)
// 	var dummy = make([]int, 15)
// 	for i := range dummy {
// 		//16回pageにtupleを入れてfullにする
// 		fmt.Print(i)
// 		bufferMg.InsertTuple(tableName, tuple)
// 	}

// 	//15の段階ではfreepageがとってこられることを確かめる
// 	pInterface, _ := bufferMg.lruCache.GetFreePD(tableName)
// 	pd, ok := pInterface.(*p.PageDescriptor)
// 	//freemapからdeleteされて持ってくるpInterfaceはnilであってほしい
// 	if !ok {
// 		t.Errorf("getFreepageError:%v", pd)
// 	}

// 	bufferMg.InsertTuple(tableName, tuple)

// 	pInterface2, _ := bufferMg.lruCache.GetFreePD(tableName)
// 	pd2, ok2 := pInterface2.(*p.PageDescriptor)
// 	//freemapからdeleteされて持ってくるpInterfaceはnilであってほしい
// 	if ok2 {
// 		t.Errorf("invalidPageDescriptorError:%v", pd2)
// 	}

// }

func TestFetchAll(t *testing.T) {
	bufferMg := NewBufferPoolManager(1000)

	page := p.NewPage()
	bufferMg.InsertPage(bufferMg.maxPid, tableName, page)

	page = p.NewPage()
	bufferMg.InsertPage(bufferMg.maxPid, tableName, page)

	//2page作って、17個insertする(1page+1個)、それで17個tupleが帰ってくれば成功
	var dummy = make([]int, 17)
	for i := range dummy {
		var tupleData = []interface{}{i, "tuple"}
		var tuple *tup.Tuple
		tuple = tup.NewTuple(tupleData)
		bufferMg.InsertTuple(tableName, tuple)
	}
	tuples := bufferMg.FetchAll(tableName, false)

	if len(tuples) != 17 {
		t.Errorf("tupleLenError,actual:%d,want:%d", len(tuples), 17)
	}

}
