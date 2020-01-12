package front

import (
	"errors"
	"fmt"
	"sort"

	"github.com/stonelike/synchroDB/buffer/btree"

	"github.com/stonelike/synchroDB/buffer"
	"github.com/stonelike/synchroDB/disk/meta"
	"github.com/stonelike/synchroDB/disk/tuple"
)

const (
	databaseName = "tempDatabase"
)

type frontManager struct {
	//catalogをここに入れる？
	catalog *meta.Catalog
	buf     *buffer.BufferPoolManager
}

type WhereStatement struct {
	column    string
	conpareOp string
	target    interface{}
}

func NewfrontManager() *frontManager {

	catalog, err := meta.LoadCatalog(databaseName)
	//loadできなかった場合は新たに制作
	if err != nil {
		catalog = meta.NewCatalog()
		//ここで永続化する？
		meta.SaveCatalog(databaseName, catalog)
	}

	return &frontManager{
		catalog: catalog,
		buf:     buffer.NewBufferPoolManager(1000),
	}
}

type Column struct {
	ColumnName  []string
	ColumnType  []meta.ColType
	ColumnValue []interface{}
}

//btree用にColumnを内包したpairを作ってあげる
type BtreeColumnPair struct {
	Key   interface{}
	Value *Column
}

//indexに設定したcolumnNameにおいて比較したい
func (a BtreeColumnPair) Less(b btree.Item) bool {
	//KeyはいまのところintOrString

	switch a.Key.(type) {
	case int32:
		aKey, okA := a.Key.(int32)
		bKey, okB := b.(*BtreeColumnPair).Key.(int32)

		if okA && okB {
			return aKey < bKey
		}

	case string:
		aKey, okA := a.Key.(string)
		bKey, okB := b.(*BtreeColumnPair).Key.(string)
		if okA && okB {
			return aKey < bKey
		}
	}
	return false
}

//updateNumはfrontでは使わないのでいらない
func (f *frontManager) TupleToColumn(tableName string, tuples []*tuple.Tuple) ([]*Column, error) {
	s := f.catalog.FetchScheme(tableName)
	if s == nil {
		return nil, fmt.Errorf("table %s is not exists", tableName)
	}

	if len(tuples[0].Data) != len(s.ColumnNames) {
		return nil, errors.New("column length is different")
	}

	for i, v := range tuples[0].Data {
		vv := v
		switch vv.Type {
		case 0: //Int
			if s.ColumnTypes[i] != meta.Int {
				return nil, errors.New("column type is different")

			}
		case 1: //String
			if s.ColumnTypes[i] != meta.VarChar {
				return nil, errors.New("column type is different")

			}
		}

	}

	var columns []*Column
	for _, v := range tuples {
		vv := v
		columnValue := extractValueFromTupleData(vv)
		columns = append(columns, &Column{
			ColumnName:  s.ColumnNames,
			ColumnType:  s.ColumnTypes,
			ColumnValue: columnValue,
		})
	}
	return columns, nil
}

func extractValueFromTupleData(tuple *tuple.Tuple) []interface{} {
	var columnValue []interface{}
	for _, v := range tuple.Data {
		vv := v
		switch vv.Type {
		case 0:
			columnValue = append(columnValue, vv.Number)
		case 1:
			columnValue = append(columnValue, vv.String_)

		}
	}

	return columnValue
}

func (f *frontManager) CreateTable(tableName string, columnNames []string, columnTypes []meta.ColType) error {
	//存在する場合はerror
	if f.catalog.HasScheme(tableName) {
		return fmt.Errorf("table %s is already exists", tableName)
	}
	scheme := meta.NewScheme(tableName, columnNames, columnTypes)
	f.catalog.AddScheme(scheme)
	//ここでcatalog永続化すべきか、pageを永続化するのと同じタイミングでいいのか？
	meta.SaveCatalog(databaseName, f.catalog)
	return nil
}

func (f *frontManager) Join(leftTableName, rightTableName string) ([]*Column, error) {
	//joinでのindexはまた今度
	leftColumn, err := f.Query(leftTableName, nil)
	if err != nil {
		return nil, err
	}
	leftColumn, err = f.columnSort(leftColumn, "id", leftTableName)
	if err != nil {
		return nil, err
	}

	rightColumn, err := f.Query(rightTableName, nil)

	if err != nil {
		return nil, err
	}
	rightColumn, err = f.columnSort(rightColumn, "id", rightTableName)
	if err != nil {
		return nil, err
	}
	//引数６はさすがに多すぎだから構造体かbuilderかにしてもいいかも
	joinedColumn, err := f.joinTwoColumn(leftColumn, rightColumn, "id", leftTableName, "id", rightTableName)

	if err != nil {
		return nil, err
	}

	return joinedColumn, nil
}

func (f *frontManager) joinTwoColumn(leftColumn, rightColumn []*Column, leftJoinColumn, leftTableName, rightJoinColumn, rightTableName string) ([]*Column, error) {
	//joinするときはrightのjoin_columnが重複付加とする
	//rightのidとleftのidをkeyにjoinする
	//新しいcolumnにはidを絶対入れて残りはお互いそれぞれidを除いたやつを入れていく

	leftColumnNumber, isExist := f.checkColumnIsExist(leftTableName, nil, leftJoinColumn)
	if !isExist {

		return nil, errors.New("left column is not exist")

	}
	rightColumnNumber, isExist := f.checkColumnIsExist(rightTableName, nil, rightJoinColumn)
	if !isExist {

		return nil, errors.New("right column is not exist")

	}

	//joinしたcolumnは一時的だからcolumnの順番とかは気にしなくてよさそう
	var newColumn []*Column
	//rightは重複なし
	for _, v := range rightColumn {
		right := v
		//leftは重複ありで先に動かす
		for _, w := range leftColumn {
			left := w
			//今回はrightもleftもidの値が同じならばという条件でjoin
			//ここのcolumnValueは両方ともidの値
			if right.ColumnValue[leftColumnNumber] == left.ColumnValue[rightColumnNumber] {
				appendColumn := createJoinedColumn(right, left, rightColumnNumber, leftColumnNumber)
				newColumn = append(newColumn, appendColumn)
			}

		}
	}

	return newColumn, nil
}

func createJoinedColumn(right, left *Column, rightColumnNumber, leftColumnNumber int) *Column {
	columnName := []string{}
	columnType := []meta.ColType{}
	columnValue := []interface{}{}
	//idだけは先に処理しておく
	columnName = append(columnName, "id")
	columnType = append(columnType, meta.Int)
	//左も右も同じなのでどっちのidでもいい
	columnValue = append(columnValue, right.ColumnValue[rightColumnNumber])

	//あとは互いにidを除いた残りを入れていく

	for i, v := range right.ColumnName {
		//今の場合だとidだったらスルー
		if v == right.ColumnName[rightColumnNumber] {
			continue
		}
		columnName = append(columnName, right.ColumnName[i])
		columnType = append(columnType, right.ColumnType[i])
		columnValue = append(columnValue, right.ColumnValue[i])
	}

	for j, w := range left.ColumnName {
		//今の場合だとidだったらスルー
		if w == left.ColumnName[leftColumnNumber] {
			continue
		}
		columnName = append(columnName, left.ColumnName[j])
		columnType = append(columnType, left.ColumnType[j])
		columnValue = append(columnValue, left.ColumnValue[j])
	}

	column := &Column{
		ColumnName:  columnName,
		ColumnType:  columnType,
		ColumnValue: columnValue,
	}

	return column
}

func (f *frontManager) checkColumnIsExist(tableName string, columns []*Column, column string) (int, bool) {
	//tableNameOrColumnでチェック
	if tableName == "" {
		for i, v := range columns[0].ColumnName {
			if v == column {
				return i, true
			}
		}
	}

	s := f.catalog.FetchScheme(tableName)
	for i, v := range s.ColumnNames {
		if v == column {
			return i, true
		}
	}
	return 0, false
}

func (f *frontManager) columnSort(columns []*Column, sortPivot, tableName string) ([]*Column, error) {
	columnNumber, isExist := f.checkColumnIsExist(tableName, nil, sortPivot)

	if !isExist {

		return nil, errors.New("column is not exist")

	}
	sort.Slice(columns, func(i, j int) bool {
		//sortPivotを基準にsort
		//columnTypeだったらIntとかVarCharだけどColumnValueでやってるので普通のintとかstringでいい
		switch columns[i].ColumnValue[columnNumber].(type) {
		case int:
			columnValue1, _ := columns[i].ColumnValue[columnNumber].(int)
			columnValue2, ok2 := columns[i].ColumnValue[columnNumber].(int)
			if ok2 {
				return columnValue1 < columnValue2
			}
		case string:
			columnValue1, _ := columns[i].ColumnValue[columnNumber].(string)
			columnValue2, ok2 := columns[i].ColumnValue[columnNumber].(string)
			if ok2 {
				return columnValue1 < columnValue2
			}

		}

		return false

	})
	return columns, nil
}

func (f *frontManager) Where(columns []*Column, whereStatement WhereStatement) ([]*Column, error) {
	//selectでtupleが来なかった場合error
	if len(columns) == 0 {
		return nil, errors.New("columnsLen is 0")
	}

	var filteredColumns []*Column

	//columnsとwhereStatementを受け取って、まずcolumnにwhereStatementのcolumnがあるか確認,確認したらその配列の番号を取得
	columnNumber, err := getColumnNumberFromColumn(columns, whereStatement.column)

	if err != nil {
		return nil, err
	}

	//ここからtuplesのfilter
	for _, column := range columns {

		isAddColumn := compareTarget(column.ColumnValue[columnNumber], whereStatement)
		if isAddColumn {
			filteredColumns = append(filteredColumns, column)
		}
	}

	return filteredColumns, nil

}

func getColumnNumberFromColumn(columns []*Column, columnName string) (int, error) {
	for i, v := range columns[0].ColumnName {
		if v == columnName {
			return i, nil
		}
	}
	return 0, errors.New("this column is not exist")
}

func compareTarget(columnValue interface{}, whereStatement WhereStatement) bool {
	//columnValueで分けているので普通にstringとかint
	switch columnValue.(type) {
	case int32:
		columnInt, ok := columnValue.(int32)

		if ok {

			return compareInt(columnInt, whereStatement)
		}

	case string:
		columnString, ok := columnValue.(string)
		if ok {
			return compareString(columnString, whereStatement)
		}
	}
	return false
}

func compareString(columnString string, whereStatement WhereStatement) bool {
	target, ok := whereStatement.target.(string)
	if !ok {
		//これまでで絶対ここに来るのはintなので
		panic("target is invalid")
	}
	//like検索はあとで、やるとしたらtry木とかに乗っければいいか？
	if columnString == target {
		return true
	}
	return false
}

func compareInt(columnInt int32, whereStatement WhereStatement) bool {
	target, ok := whereStatement.target.(int)
	if !ok {
		//これまでで絶対ここに来るのはintなので
		panic("target is invalid")
	}
	switch whereStatement.conpareOp {
	case ">":
		if columnInt > int32(target) {
			return true
		}
	case "=":
		if columnInt == int32(target) {
			return true
		}
	case "<":
		if columnInt < int32(target) {
			return true
		}
	}

	return false
}

func getTargetColumnData(columnName string, s *meta.Scheme) (meta.ColType, int, error) {
	for i, v := range s.ColumnNames {
		if columnName == v {

			return s.ColumnTypes[i], i, nil
		}
	}

	return 0, 0, errors.New("columnName is not exists")
}

func (f *frontManager) Select(columnNames []string, columns []*Column) ([]*Column, error) {

	var columnNumbers []int
	for _, v := range columnNames {
		columnNumber, isExist := f.checkColumnIsExist("", columns, v)

		if !isExist {
			return nil, errors.New("this column is not exist")
		}
		columnNumbers = append(columnNumbers, columnNumber)
	}

	//tempColumnをつくる
	var newColumn []*Column

	for _, v := range columns {
		vv := v
		selectedColumn := createSelectedColumn(vv, columnNumbers)
		newColumn = append(newColumn, selectedColumn)
	}

	return newColumn, nil
}

func createSelectedColumn(column *Column, columnNumbers []int) *Column {
	columnName := []string{}
	columnType := []meta.ColType{}
	columnValue := []interface{}{}
	//columnNumberのrowだけとりたい
	for i := range column.ColumnName {
		if contains(columnNumbers, i) {
			//この時だけほしい
			columnName = append(columnName, column.ColumnName[i])
			columnType = append(columnType, column.ColumnType[i])
			columnValue = append(columnValue, column.ColumnValue[i])
		}
	}

	newColumn := &Column{
		ColumnName:  columnName,
		ColumnType:  columnType,
		ColumnValue: columnValue,
	}

	return newColumn
}

func contains(s []int, e int) bool {
	for _, v := range s {
		if e == v {
			return true
		}
	}

	return false
}

type JoinStatement struct {
	leftTableName  string
	rightTableName string
}

type SelectStatement struct {
	selectColumns  []string
	joinStatement  JoinStatement
	whereStatement WhereStatement
	from           string
}

type Option func(*SelectStatement)

func (f *frontManager) SetSelectColumns(columnNames []string) Option {
	return func(selectSt *SelectStatement) {
		selectSt.selectColumns = columnNames
	}
}

func (f *frontManager) SetJoinStatement(leftTableName, rightTableName string) Option {
	return func(selectSt *SelectStatement) {
		selectSt.joinStatement = JoinStatement{
			leftTableName:  leftTableName,
			rightTableName: rightTableName,
		}
	}
}

func (f *frontManager) SetWhereStatement(column, columnOp string, target interface{}) Option {
	return func(selectSt *SelectStatement) {
		selectSt.whereStatement = WhereStatement{
			column:    column,
			conpareOp: columnOp,
			target:    target,
		}
	}
}

func (f *frontManager) SetFrom(tableName string) Option {
	return func(selectSt *SelectStatement) {
		selectSt.from = tableName
	}
}

func (f *frontManager) SelectQuery(options ...Option) ([]*Column, error) {
	selectSt := []string{}
	selectStatement := &SelectStatement{
		selectColumns: selectSt,
		joinStatement: JoinStatement{
			leftTableName:  "",
			rightTableName: "",
		},
		whereStatement: WhereStatement{
			column: "",
		},
		from: "",
	}

	//引数に応じてselectStatementを更新
	for _, option := range options {
		option(selectStatement)
	}

	if selectStatement.from == "" && selectStatement.joinStatement.leftTableName == "" {
		//fromもしくはJoinは絶対必要
		panic("requireTableName")
	}

	//joinがある場合はjoin→where→select
	//ない場合はwhere→select

	var newColumns []*Column
	if selectStatement.joinStatement.leftTableName != "" {
		//joinが指定されている
	}

	if selectStatement.whereStatement.column != "" {
		//whereが指定されている,この場合はその指定されたcolumnにindexが付与されていればとってくるのはbtreeから

		//len0はここが初めてということ
		if len(newColumns) == 0 {
			//whereからスタートの場合indexチェックして、queryでとってきてもうBtreeにcolumnがはいっているのでwhere通さないで終わり
			newIndexColumns, err := f.Query(selectStatement.from, selectStatement.whereStatement)
			if err != nil {
				return nil, err
			}
			newColumns = newIndexColumns

		} else {
			newwhereColumns, err := f.Where(newColumns, selectStatement.whereStatement)

			if err != nil {
				return nil, err
			}
			newColumns = newwhereColumns
		}

	}

	//selectこのままじゃjoinに対応してない,tableNameからcolumnがあるか確認してしまっているので、そうではなく、どうせselectはどこかからcolumnをとってきた後なので、そのcolumnにcolumnNameがあるか確認する
	if len(selectStatement.selectColumns) != 0 {
		//とってくるcolumnが指定されている
		//ここが初めての場合(以前までで結果が空白になってもerrorでかえすようにすればいい)
		if len(newColumns) == 0 {
			newQueryColumns, err := f.Query(selectStatement.from, nil)
			if err != nil {
				return nil, err
			}

			newColumns = newQueryColumns
		} else {
			newSelectedColumn, err := f.Select(selectStatement.selectColumns, newColumns)

			if err != nil {
				return nil, err
			}
			newColumns = newSelectedColumn
		}

	}

	//とくに条件がない場合
	if len(newColumns) == 0 {
		simpleColumns, err := f.Query(selectStatement.from, nil)
		if err != nil {
			return nil, err
		}

		return simpleColumns, nil
	}

	return newColumns, nil
}

//これ全データとってくるだけだからselectから名前変えたほうがよさそう
//selectを通してcolumnが返るようにする
//selectにはselect　*　from　tableName　where　~などが必要
func (f *frontManager) Query(tableName string, whereStatement interface{}) ([]*Column, error) {
	//このtableの全pageをとってきてcolumnの形なりにして[]columnみたいなかたちに整形してあげる
	//orderByをするときはexternalMergeで
	//emptyTupleではなくusedTupleがほしい
	var newColumns []*Column
	if whereStatement == nil {
		tuples := f.buf.FetchAll(tableName, false)

		newColumns, err := f.TupleToColumn(tableName, tuples)

		if err != nil {
			return nil, err
		}
		return newColumns, nil
	}

	whereSt, ok := whereStatement.(WhereStatement)

	if !ok {
		return nil, errors.New("wrongWhereStatementError")
	}

	//ここでwhere条件を一気に満たしてしまえばよい、idでindexを組んでいるならid>2以上のをとってくるみたいな
	_, isIndexed := f.checkColumnHasIndex(tableName, whereSt.column)
	//b+treeじゃないとたぶん不等号検索は意味ないので今は等号検索のみ
	if isIndexed {

		btreePair := &BtreeColumnPair{
			Key: whereSt.target,
		}
		columnPair, isExist := f.buf.BTreeMap[tableName+whereSt.column].Search(btreePair)

		if isExist {
			newColumns = append(newColumns, columnPair.(*BtreeColumnPair).Value)
			return newColumns, nil
		}
	}

	return newColumns, errors.New("column is empty")

}

func (f *frontManager) checkColumnHasIndex(tableName, column string) (int, bool) {
	s := f.catalog.FetchScheme(tableName)
	for i, v := range s.ColumnNames {
		if v == column {

			return i, s.UseIndex[i]
		}
	}
	return 0, false
}

func (f *frontManager) AddIndex(tableName, columnName string) bool {
	columnNumber, isExist := f.checkColumnIsExist(tableName, nil, columnName)
	if !isExist {
		return false
	}
	s := f.catalog.FetchScheme(tableName)

	s.UseIndex[columnNumber] = true
	//diskに書き込み
	meta.SaveCatalog(databaseName, f.catalog)

	//diskからbtreeをとってくるかは後で検討、とりあえず選択したtableNameのcolumnにbtreeがなければつくる

	return f.createBtree(tableName, columnName)
}

func (f *frontManager) createBtree(tableName, columnNane string) bool {
	//もうすでにあれば何もしない
	if _, ok := f.buf.BTreeMap[tableName+columnNane]; ok {
		return true
	}

	f.buf.CreateBTree(tableName, columnNane)
	return f.insertAllDataToBTree(tableName, columnNane)

}

func (f *frontManager) insertAllDataToBTree(tableName, columnName string) bool {
	//columnNameをbtreeのkey、columnをvalueとしてbtreeを構築
	columns, err := f.Query(tableName, nil)
	if err != nil {
		return false
	}

	columnNumber, isExist := f.checkColumnIsExist(tableName, nil, columnName)
	if !isExist {
		return false
	}

	btree := f.buf.BTreeMap[tableName+columnName]
	for _, v := range columns {
		vv := v

		columnPair := &BtreeColumnPair{
			//実際の値がkey、例えばidなら1とか

			Key:   vv.ColumnValue[columnNumber],
			Value: vv,
		}

		btree.Insert(columnPair)
	}

	return true

}

func (f *frontManager) Insert(tableName string, columnNames []string, values []interface{}) error {

	s := f.catalog.FetchScheme(tableName)
	if s == nil {
		return fmt.Errorf("table %s is not exists", tableName)
	}

	if len(values) != len(s.ColumnNames) {
		return errors.New("column length is different")
	}

	for i, _ := range columnNames {
		if s.ColumnNames[i] != columnNames[i] {
			return errors.New("columnOrder or columnName is Incorrect")
		}

		if s.ColumnTypes[i] == meta.Int {
			if _, ok := values[i].(int); !ok {
				return errors.New("columnType is Incorrect")
			}
		}

		if s.ColumnTypes[i] == meta.VarChar {
			if _, ok := values[i].(string); !ok {
				return errors.New("columnType is Incorrect")

			}
		}

	}

	//追加する際にBtreeにもinsert

	t := tuple.NewTuple(values)
	f.buf.InsertTuple(tableName, t)
	return nil
}
