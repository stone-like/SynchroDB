package front

import (
	"testing"
)

const (
	tableName = "tempTable"
)

//tempDatabaseにはtempTableがいまのところ入っている
func TestInsertAndSelect(t *testing.T) {
	fm := NewfrontManager()
	nameSlice := []string{"id", "name"}
	valueSlice := []interface{}{1, "max"}
	fm.Insert(tableName, nameSlice, valueSlice)
	nameSlice = []string{"id", "name"}
	valueSlice = []interface{}{2, "tai"}
	fm.Insert(tableName, nameSlice, valueSlice)

	where := WhereStatement{
		column:    "id",
		conpareOp: "=",
		target:    2,
	}
	columns, err := fm.Select(tableName)

	if err != nil {
		t.Error("can't get proper column")

	}
	column, err := fm.Where(columns, where)

	if err != nil {
		t.Error("can't get proper tuple")
	}

	//今回ほしいtupleは一つなので
	if len(column) != 1 {
		t.Errorf("columnLenError: actual:%v,want:%v", len(column), 1)
	}

	if column[0].ColumnValue[0] != int32(2) {
		t.Errorf("columnIdError: actual:%v,want:%v", column[0].ColumnValue[0], 2)
	}

	if column[0].ColumnValue[1] != "tai" {
		t.Errorf("columnIdError: actual:%v,want:%v", column[0].ColumnValue[1], "tai")
	}

}

func TestJoin(t *testing.T) {
	fm := NewfrontManager()

	nameSlice := []string{"id", "name"}
	valueSlice := []interface{}{1, "max"}
	fm.Insert(tableName, nameSlice, valueSlice)
	nameSlice = []string{"id", "name"}
	valueSlice = []interface{}{2, "cli"}
	fm.Insert(tableName, nameSlice, valueSlice)
	nameSlice = []string{"id", "name"}
	valueSlice = []interface{}{3, "pp"}
	fm.Insert(tableName, nameSlice, valueSlice)

	nameSlice = []string{"id", "course"}
	valueSlice = []interface{}{1, "English"}
	fm.Insert("course", nameSlice, valueSlice)
	nameSlice = []string{"id", "course"}
	valueSlice = []interface{}{1, "Math"}
	fm.Insert("course", nameSlice, valueSlice)
	nameSlice = []string{"id", "course"}
	valueSlice = []interface{}{2, "English"}
	fm.Insert("course", nameSlice, valueSlice)
	nameSlice = []string{"id", "course"}
	valueSlice = []interface{}{2, "Math"}
	fm.Insert("course", nameSlice, valueSlice)

	//id3はjoinされないので、合計4つ取得したい
	joinedcolumn, err := fm.Join("course", tableName)

	if err != nil {
		t.Errorf("joinError:%v", err)
	}

	if len(joinedcolumn) != 4 {
		t.Errorf("queryLenError: actual:%v,want:%v", len(joinedcolumn), 4)
	}

	where := WhereStatement{
		column:    "id",
		conpareOp: "=",
		target:    2,
	}

	filteredcolumn, err := fm.Where(joinedcolumn, where)
	if err != nil {
		t.Errorf("filteredError:%v", err)
	}

	if len(filteredcolumn) != 2 {
		t.Errorf("queryLenError: actual:%v,want:%v", len(filteredcolumn), 2)
	}

}
