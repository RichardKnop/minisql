package minisql

const (
	SchemaTableName    = "minisql_schema"
	MaxColumns         = 64
	RootPageConfigSize = 100
)

func columnNames(columns ...Column) []string {
	names := make([]string, 0, len(columns))
	for _, aColumn := range columns {
		names = append(names, aColumn.Name)
	}
	return names
}
