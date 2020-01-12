package page

import (
	tuple "github.com/stonelike/synchroDB/disk/tuple"
)

//1pageに最大16個tupleが入る,1pageは16kb
const (
	MaxTupleNumber = 16
)

type PageDescriptor struct {
	IsDirty   bool
	PageID    uint64
	Page      *Page
	TableName string
	PinCount  uint64
}

type Page struct {
	Tuples [MaxTupleNumber]tuple.Tuple
}

func (p *Page) ExtractEmptyTuple() []*tuple.Tuple {
	var tuples []*tuple.Tuple
	for _, v := range p.Tuples {
		vv := v
		if v.UpdateNum == 1 {
		} else {
			tuples = append(tuples, &vv)
		}
	}
	return tuples
}

func (p *Page) ExtractUsedTuple() []*tuple.Tuple {
	var tuples []*tuple.Tuple
	for _, v := range p.Tuples {
		//forのvでポインタを扱うとどんどん新しいやつにセットされてしまうので、一旦別変数をはさんであげる
		vv := v
		if v.UpdateNum == 0 {
		} else {

			tuples = append(tuples, &vv)

		}
	}

	return tuples
}

func (p *Page) HasEmptyTuple() (bool, int) {
	for i, v := range p.Tuples {
		if v.UpdateNum == 0 {
			return true, i
		}
	}

	return false, 0 //本当は0だとtuple[0]になってしまうからダメなんだけど、一つ目のboolで判別してから、二つ目のintを使う
}

func NewPage() *Page {
	return &Page{
		Tuples: [MaxTupleNumber]tuple.Tuple{},
	}
}

func SerializePage(p *Page) ([16384]byte, error) {

	var b [16384]byte
	for i, t := range p.Tuples {
		tupleBytes, err := tuple.SerializeTuple(&t)
		if err != nil {
			return b, err
		}
		copy(b[i*1024:i*1024+1024], tupleBytes[:])
	}

	return b, nil
}

func DeserializePage(b [16384]byte) (*Page, error) {
	p := &Page{}
	for i := 0; i < MaxTupleNumber; i++ {
		var temp [1024]byte
		//1024byteずつdeserializeしていく,sync.Poolとか使ってもいいかもしれないけど今回は最適化とかするわけじゃないので毎回tempを生成する
		copy(temp[:], b[i*1024:i*1024+1024])
		t, err := tuple.DeserializeTuple(temp)

		if err != nil {
			//errorした場合は空tupleでも入れる？
			// return nil, err
		}

		p.Tuples[i] = *t
	}

	return p, nil
}
