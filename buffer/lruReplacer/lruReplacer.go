package lrureplacer

import (
	"container/list"

	"github.com/stonelike/synchroDB/disk/tuple"

	exhash "github.com/stonelike/synchroDB/buffer/exHash"
	p "github.com/stonelike/synchroDB/disk/page"
	util "github.com/stonelike/synchroDB/util"
)

type LRUCache struct {
	cap       uint64
	cachelist *list.List
	exHash    *exhash.ExHash
	//tableName+pageIDで作っているのでそのhash値をfreemapのkeyでいいかな？
	freemap map[string]map[uint64]*Pair
}

type Pair struct {
	key   uint64
	value interface{}
}

//これそもそもhashの量に対してcapを合わせる必要ある？capがhashに対して余ることになるけどそれはそれでいいし、overしたら除去してやりくりするだけ

//lockはここでかければいい、lruの操作時に合わせてexHashも操作するため
func NewLRU(cap uint64) *LRUCache {
	var hashGlobalDepth uint64 = 1
	// var hashDirectory uint64 = 1 << hashGlobalDepth
	var hashBucketSize uint64 = 4

	return &LRUCache{
		cap:       cap,
		cachelist: list.New(),
		exHash:    exhash.NewExHash(hashGlobalDepth, hashBucketSize),
		freemap:   make(map[string]map[uint64]*Pair),
	}
}

func (l *LRUCache) GetAll(tableName string, wantEmpty bool) []*tuple.Tuple {
	var tupleSlice []*tuple.Tuple

	//どこかのタイミングでソートしてもいいかもしれない

	var i uint64
	for i = 0; ; i++ {
		//exHashを0から順々に調べていくわけだけど、存在しないpageになったらnilが返るのでそこで終わり
		//ただとってきたPageには使っていないtupleも含まれるのでいっそtupleの配列にしてしまったほうがいいかも
		pageD := l.Get(uint64(i), tableName)
		if pageD == nil {
			break
		}

		//有効なページをすべて取ってこられたら,さらにその中から有効なタプルを取り出す

		tupleSlice = append(tupleSlice, extractTuple(pageD, wantEmpty)...)
	}

	return tupleSlice
}

func extractTuple(pageD interface{}, wantEmpty bool) []*tuple.Tuple {

	pageDescriptor := pageD.(*p.PageDescriptor)

	if wantEmpty {
		return pageDescriptor.Page.ExtractEmptyTuple()
	} else {
		return pageDescriptor.Page.ExtractUsedTuple()
	}

}

//keyもvalueもinterface{}にしておけばいいかな？
//pdを返したい
func (l *LRUCache) Get(pageID uint64, tableName string) interface{} {

	key := util.Hash(pageID, tableName)
	listElem, isExist := l.exHash.Get(key)
	// fmt.Println("key:%v", key)
	if !isExist {
		return nil
	}

	l.cachelist.MoveToFront(listElem)

	// pageD := listElem.Value.(*Pair).value
	// pd := pageD.(*p.PageDescriptor)

	// //pageがfullじゃなかったらfreelistに追加
	// isEmpty:= pd.Page.HasEmptyTuple()
	// if isEmpty {
	// 	l.freemap[tableName][key] = &Pair{
	// 		key:   key,
	// 		value: pd,
	// 	}
	// }

	return listElem.Value.(*Pair).value

}

func (l *LRUCache) Put(pageID uint64, tableName string, value interface{}) interface{} {
	key := util.Hash(pageID, tableName)
	// fmt.Println("putKey:%v", key)
	var victim interface{} //victimを必ず返すんだけど、victimがなかったらvictimはnilが返る

	//すでにhashに存在したら一番前へ
	if listElem, isExist := l.exHash.Get(key); isExist {
		l.cachelist.MoveToFront(listElem)
		//exHashUpdateでいいよね？
		listElem.Value = &Pair{key: key, value: value}
		l.exHash.Update(key, listElem)
		return victim
	}
	//まだ未登録のpageなので新しく登録

	//cachelistがfullか確認してfullだったら
	if l.isFull() {
		//exHashの削除
		victimElem := l.cachelist.Back()

		victim = victimElem.Value.(*Pair).value.(*p.PageDescriptor)

		hashKey := victimElem.Value.(*Pair).key
		victimTableName := victimElem.Value.(*Pair).value.(*p.PageDescriptor).TableName
		l.exHash.Delete(hashKey)
		//freemapからもdelete
		l.FreeMapDelete(victimTableName, hashKey)

		l.cachelist.Remove(victimElem)

	}

	//前準備が終わったのでexHashとlistにそれぞれset
	node := &Pair{
		key:   key,
		value: value,
	}

	ptr := l.cachelist.PushFront(node)
	l.exHash.Put(key, ptr)

	//freelistにも登録
	pageD := ptr.Value.(*Pair).value
	pd := pageD.(*p.PageDescriptor)
	isEmpty, _ := pd.Page.HasEmptyTuple()
	if isEmpty {
		//このときに内側のmapは初期化完了していないので
		if len(l.freemap[tableName]) == 0 {
			l.freemap[tableName] = make(map[uint64]*Pair)
			//lenが０でないときにmakeを代入してしまうと、mapをclearしてしまう
		}

		l.freemap[tableName][key] = &Pair{
			key:   key,
			value: pd,
		}
	}

	return victim
}

func (l *LRUCache) FreeMapDelete(tableName string, hashKey uint64) {
	delete(l.freemap[tableName], hashKey)
}

func (l *LRUCache) isFull() bool {

	return uint64(l.cachelist.Len()) == l.cap
}

func (l *LRUCache) GetFreePD(tableName string) (interface{}, int) {
	//これdiskからもとってきたほうがいいのかも?

	for _, v := range l.freemap[tableName] {

		isEmpty, number := v.value.(*p.PageDescriptor).Page.HasEmptyTuple()

		if isEmpty {
			return v.value, number
		}

	}

	return nil, 0
}
