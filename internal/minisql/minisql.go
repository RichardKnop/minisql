package minisql

const (
	SchemaTableName    = "minisql_schema"
	MaxColumns         = 64
	RootPageConfigSize = 100
)

func fieldsFromColumns(columns ...Column) []Field {
	fields := make([]Field, 0, len(columns))
	for _, aColumn := range columns {
		fields = append(fields, Field{Name: aColumn.Name})
	}
	return fields
}
