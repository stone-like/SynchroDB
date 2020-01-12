package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type Int64 int64

func (i Int64) Less(than Item) bool {
	l, ok := than.(Int64)
	if !ok {
		return false
	}
	return i < l
}
func TestSplit(t *testing.T) {
	btree := NewBTree(4)

	btree.Insert(Int64(1))
	btree.Insert(Int64(2))
	btree.Insert(Int64(3))
	btree.Insert(Int64(4))
	btree.Insert(Int64(5))
	btree.Insert(Int64(6))

	btree.Insert(Int64(7))

	btree.Insert(Int64(8))
	btree.Insert(Int64(9))
	btree.Insert(Int64(10))

	assert.Equal(t, btree.root.items[0], Int64(4))

	assert.Equal(t, btree.root.children[0].items[0], Int64(2))
	assert.Equal(t, btree.root.children[1].items[0], Int64(6))
	assert.Equal(t, btree.root.children[0].children[0].items[0], Int64(1))
	assert.Equal(t, btree.root.children[0].children[1].items[0], Int64(3))
	assert.Equal(t, btree.root.children[1].children[0].items[0], Int64(5))
	assert.Equal(t, btree.root.children[1].children[1].items[0], Int64(7))
	assert.Equal(t, btree.root.children[1].children[2].items[0], Int64(9))
	assert.Equal(t, btree.root.children[1].children[2].items[1], Int64(10))

}

func TestRemove(t *testing.T) {
	btree := NewBTree(4)

	btree.Insert(Int64(1))
	btree.Insert(Int64(2))
	btree.Insert(Int64(3))
	btree.Insert(Int64(4))
	btree.Insert(Int64(5))
	btree.Insert(Int64(6))
	btree.Insert(Int64(7))
	btree.Insert(Int64(8))
	btree.Insert(Int64(9))
	btree.Insert(Int64(10))
	btree.Insert(Int64(11))
	btree.Insert(Int64(12))
	btree.Insert(Int64(13))
	btree.Insert(Int64(14))

	btree.Remove(Int64(8))

	assert.Equal(t, btree.root.items[0], Int64(4))
	assert.Equal(t, btree.root.items[1], Int64(9))

	assert.Equal(t, btree.root.children[0].items[0], Int64(2))
	assert.Equal(t, btree.root.children[1].items[0], Int64(6))
	assert.Equal(t, btree.root.children[2].items[0], Int64(12))

	assert.Equal(t, btree.root.children[0].children[0].items[0], Int64(1))
	assert.Equal(t, btree.root.children[0].children[1].items[0], Int64(3))
	assert.Equal(t, btree.root.children[1].children[0].items[0], Int64(5))
	assert.Equal(t, btree.root.children[1].children[1].items[0], Int64(7))
	assert.Equal(t, btree.root.children[2].children[0].items[0], Int64(10))
	assert.Equal(t, btree.root.children[2].children[0].items[1], Int64(11))
	assert.Equal(t, btree.root.children[2].children[1].items[0], Int64(13))
	assert.Equal(t, btree.root.children[2].children[1].items[1], Int64(14))

}

func TestSearch(t *testing.T) {
	btree := NewBTree(4)

	btree.Insert(Int64(1))
	btree.Insert(Int64(2))
	btree.Insert(Int64(3))
	btree.Insert(Int64(4))
	btree.Insert(Int64(5))
	btree.Insert(Int64(6))
	btree.Insert(Int64(7))
	btree.Insert(Int64(8))
	btree.Insert(Int64(9))
	btree.Insert(Int64(10))
	btree.Insert(Int64(11))
	btree.Insert(Int64(12))
	btree.Insert(Int64(13))
	btree.Insert(Int64(14))

	btree.Remove(Int64(8))

	//delete済の8でfalseが返ってきてほしい
    testcases := []struct{
		isSuccess bool
		number Int64
	}{
		{isSuccess:true,number:Int64(7)},
		{isSuccess:false,number:Int64(8)},
	}
	
	for _,c := range testcases{
		 item,isSuccess := btree.Search(c.number)
		 if isSuccess != isSuccess{
			 t.Error("boolean is different")
			 if item != c.number{
                 t.Error("number is different")
			 }
		 }
	}

}
