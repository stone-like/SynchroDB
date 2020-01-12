package exhash

import (
	"container/list"
)

// //list.Element{
//   Value:&Pair{
// 	  key:key,
// 	  value:value
//   }
// }みたいになっている
type Bucket struct {
	depth         uint64
	maxBucketSize uint64
	bucketMap     map[uint64]*list.Element //new時にmakeしないと assignment to entry in nil mapになってしまう
}

func NewBucket(depth, bucketSize uint64) *Bucket {
	return &Bucket{
		depth:         depth,
		maxBucketSize: bucketSize,
		bucketMap:     make(map[uint64]*list.Element),
	}
}

func (b *Bucket) Search(key uint64) (*list.Element, bool) {
	if listElem, ok := b.bucketMap[key]; ok {
		return listElem, true
	} else {
		return nil, false
	}
}

func (b *Bucket) Update(key uint64, listElem *list.Element) {
	//一応確認するけどもう存在確認は終わってるはずなのでpanicでいい？
	if _, ok := b.bucketMap[key]; ok {
		b.bucketMap[key] = listElem
	} else {
		panic("invalid Update")
	}
}

func (b *Bucket) Remove(key uint64) {
	if _, ok := b.bucketMap[key]; ok {
		delete(b.bucketMap, key)
	} else {
		panic("invalid Update")
	}
}

func (b *Bucket) Insert(key uint64, listElem *list.Element) {
	_, ok := b.bucketMap[key]

	if ok {
		//updateじゃなくinsertであってほしいので
		panic("invalid Update")
	}

	if b.IsFull() {
		panic("bucket is full")
	}

	b.bucketMap[key] = listElem
}

func (b *Bucket) IsFull() bool {
	return uint64(len(b.bucketMap)) == b.maxBucketSize

}

func (b *Bucket) IncreaseDepth() uint64 {
	b.depth++
	return b.depth
}

func (b *Bucket) CopyMap() map[uint64]*list.Element {
	return b.bucketMap
}

func (b *Bucket) Clear() {
	b.bucketMap = make(map[uint64]*list.Element)
}

func (b *Bucket) GetPairIndex(maskedKey uint64) uint64 {
	var unsinged uint64 = 1
	ss := unsinged << (b.depth - 1)
	v := maskedKey ^ ss
	return v
}

func (b *Bucket) GetIndexDiff() uint64 {
	return 1 << b.depth
}
