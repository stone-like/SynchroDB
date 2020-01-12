package meta

type Scheme struct {
	TableName   string    `json:"tableName"`
	ColumnNames []string  `json:"columnNames"`
	ColumnTypes []ColType `json:"columnTypes"`
}

func NewScheme(tableName string, columnNames []string, columnTypes []ColType) *Scheme {
	return &Scheme{
		TableName:   tableName,
		ColumnNames: columnNames,
		ColumnTypes: columnTypes,
	}
}

type ColType uint8

const (
	Int ColType = iota //Intなら0,VarCharなら１
	VarChar
)
