package tuple

import (
	"testing"
)

func TestTuple(t *testing.T) {

	valueSlice := []interface{}{1, "max"}
	tup := NewTuple(valueSlice)
	//なぜかtup.Data[0].Typeはprintされないんだけどちゃんとあるから大丈夫
	if tup.Data[0].Type != 0 {
		t.Errorf("actual:%v,want:%v", tup.Data[0].Type, TupleData_INT)
	}
}
