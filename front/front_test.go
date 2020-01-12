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
	columns, err := fm.Query(tableName, nil)

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

	//selectでidとnameからしっかりnameだけとれてるか
	columnNames := []string{"name"}
	column, err = fm.Select(columnNames, column)
	if err != nil {
		t.Error("can't get name column")
	}

	//nameだけなのでlenは一つだけ
	if len(column[0].ColumnValue) != 1 {
		t.Errorf("selectLenError: actual:%v,want:%v\n", len(column[0].ColumnValue), 1)
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

func TestIndex(t *testing.T) {
	fm := NewfrontManager()
	nameSlice := []string{"id", "name"}
	valueSlice := []interface{}{1, "max"}
	fm.Insert(tableName, nameSlice, valueSlice)
	nameSlice = []string{"id", "name"}
	valueSlice = []interface{}{2, "cli"}
	fm.Insert(tableName, nameSlice, valueSlice)

	//ここでidにindexを貼ると上の二つがBtreeに組み込まれてほしい
	fm.AddIndex(tableName, "id")
	//searchにはBtrePairのKeyしか使わないのでValueは空欄でいいはず

	//keyはちゃんとcolumnType似合ってないとダメ見たい
	btreePair := &BtreeColumnPair{
		Key: int32(1),
	}
	columnPair, isExist := fm.buf.BTreeMap[tableName+"id"].Search(btreePair)

	if !isExist {
		t.Error("wantColumn is not exist")
	}

	column := columnPair.(*BtreeColumnPair).Value

	if column.ColumnValue[0] != int32(1) {
		t.Errorf("IdnumberError: actual:%v,want:%v\n", column.ColumnValue[0], 1)
	}

}

func TestSelectQuery(t *testing.T) {

	fm := NewfrontManager()
	nameSlice := []string{"id", "name"}
	valueSlice := []interface{}{1, "max"}
	fm.Insert(tableName, nameSlice, valueSlice)
	nameSlice = []string{"id", "name"}
	valueSlice = []interface{}{2, "cli"}
	fm.Insert(tableName, nameSlice, valueSlice)

	fm.AddIndex(tableName, "id")

	selectColumns := []string{"name"}
	columns, err := fm.SelectQuery(fm.SetSelectColumns(selectColumns), fm.SetWhereStatement("id", "=", int32(2)), fm.SetFrom(tableName))
	//whereでid2のをとってrowはnameだけになっているか確認

	if err != nil {
		t.Errorf("selectQueryError:%v\n", err)
	}

	if len(columns) != 1 {
		t.Errorf("columnLenError: actual:%v,want:%v\n", len(columns), 1)
	}

	if columns[0].ColumnValue[0] != "cli" {
		t.Errorf("columnNameError: actual:%v,want:%v\n", columns[0].ColumnValue[0], "cli")
	}
}
