package exhash

import (
	"container/list"
)

//keyはどうせhashをbufferpoolmanagerで行うのでuint64固定でいいかな？

type Directory struct {
	globalDepth   uint64
	maxBucketSize uint64
	buckets       []*Bucket
}

func NewDirectory(depth, bucketSize uint64) *Directory {

	//最初は２つ
	initialDirSize := 1 << depth
	var buckets []*Bucket

	for i := 0; i < initialDirSize; i++ {
		buckets = append(buckets, NewBucket(depth, bucketSize))
	}
	//bucket
	return &Directory{
		globalDepth:   depth,
		maxBucketSize: bucketSize,
		buckets:       buckets,
	}
}

//ここでhashkeyつくりたいんだけどtablenameが必要なんだよなぁ・・・いや、keyをconcatinateまでしておいて、hash化をここですればいい？
func (d *Directory) Mask(key uint64) uint64 {

	return key & (1<<d.globalDepth - 1)
}

func (d *Directory) Search(key uint64) (*list.Element, bool) {

	//keyBytesが来るので、あとはhash化とmaskするだけ
	//bucketを特定するのはmask済みのkey、bucketからvalueを取り出すのはkey
	maskedKey := d.Mask(key)
	listElem, isExist := d.buckets[maskedKey].Search(key)

	return listElem, isExist
}

func (d *Directory) Update(key uint64, listElem *list.Element) {
	maskedKey := d.Mask(key)
	//update呼ぶときはもうすでにLruでgetを読んで存在確認している時だからここで存在確認はいらない
	d.buckets[maskedKey].Update(key, listElem)
}

func (d *Directory) Remove(key uint64) {
	maskedKey := d.Mask(key)
	b := d.buckets[maskedKey]
	b.Remove(key)
	//本当はここでmodeによってはmergeかshrinkするんだけど、今回はどっちもいらない
	//mCountは何に使うんだろ
	//mCount++
}

func (d *Directory) Insert(key uint64, listElem *list.Element, reinserted bool) {
	maskedKey := d.Mask(key)
	b := d.buckets[maskedKey]

	//すでにbucketがいっぱいだったらsplitしてからinsert
	if b.IsFull() {
		d.Split(maskedKey)
		d.Insert(key, listElem, reinserted)
		return
	}

	//fullではないとき
	b.Insert(key, listElem)

	//普通のinsertのときはcountが増えるみたいだけど・・・？
	if !reinserted {
		//mCount++
	}

}

func (d *Directory) Split(maskedKey uint64) {
	//splitの時は問題のbucketがfullのとき

	//bucketのdepthを一つ増やす
	localDepth := d.buckets[maskedKey].IncreaseDepth()

	if localDepth > d.globalDepth {
		d.Grow()
	}

	//ここからbucketをsplitしていく,新たに作ったbucketの初期localdepthはsplit前の増えた奴と同じになる,bucketそのものじゃなくbucketMapをとってくる
	tempBucketMap := d.buckets[maskedKey].CopyMap()
	//コピー元を空に
	d.buckets[maskedKey].Clear()

	newBucket := NewBucket(localDepth, d.maxBucketSize)
	//newBucketに元の値をコピー
	d.assignToSiblings(maskedKey, newBucket)

	//splitする元のbucketをinsertするんだけど、もうsplitした後だから例えば末尾が同じ０でおんなじところに入っていたやつでも今はglobaldepthが2の状態でmaskされるので10と00では違うbucketに分かれて入ることとなる
	for key, value := range tempBucketMap {
		//reinsertedをtrueに
		d.Insert(key, value, true)
	}
}

func (d *Directory) assignToSiblings(maskedKey uint64, bucket *Bucket) {
	//空にしたbucketから取得
	pairIndex := d.buckets[maskedKey].GetPairIndex(maskedKey)
	//   indexDiff := d.buckets[maskedKey].GetIndexDiff()
	//   dirSize = d.getDirSize()

	//splitした後のindexを持ってきて、そこにつながるbucketを設定する
	d.buckets[pairIndex] = bucket

	//ここはいらないよね・・・？
	//   for (i:= pairIndex -indexDiff; i >=0;i -= indexDiff){
	// 	  d.buckets[i] = bucket
	//   }

	//   for (i := pairIndex + indexDiff; i < dirSize; i += indexDiff){
	// 	  d.buckets[i] = bucket
	//   }
}

func (d *Directory) Grow() {
	dirSize := d.getDirSize()
	//bucket数をdirSize*2に増やすなので、depth1から2になるときはdirSizeが２なので2*2で４になるんだけどgolangではsliceを使っているので別に拡張する必要はない(メモリ効率は悪いんだけど)

	//これは[0,1,2,3]になるんだけど0,1が今まで通りで、2に0のbucketが3に1のbucketが入る
	var i uint64
	for i = 0; i < dirSize; i++ {
		//ここは文字で表しづらいので図に書くこととする
		d.buckets = append(d.buckets, d.buckets[i])
	}

	d.globalDepth++

}

func (d *Directory) getDirSize() uint64 {
	return 1 << d.globalDepth
}

//Countはinsertすると+1、removeすると-1するだけだからいらないかな
