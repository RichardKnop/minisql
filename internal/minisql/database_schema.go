package minisql

import (
	"fmt"
)

const (
	SchemaTableName    = "minisql_schema"
	MaxColumns         = 64
	RootPageConfigSize = 100
)

type SchemaType int

const (
	SchemaTable SchemaType = iota + 1
	SchemaPrimaryKey
	SchemaUniqueIndex
	SchemaSecondaryIndex
)

type Schema struct {
	Type      SchemaType
	Name      string
	TableName string
	RootPage  PageIndex
	DDL       string
}

var (
	// TODO - do we need to limit SQL schemas to fit into a single page?
	maximumSchemaSQL = UsablePageSize -
		4 - //  type column
		2*(varcharLengthPrefixSize+MaxInlineVarchar) - // name and tbl_name columns
		4 - // root_page column
		(varcharLengthPrefixSize + 4) - // sql column
		RootPageConfigSize
	mainTableColumns = []Column{
		{
			Kind: Int4,
			Size: 4,
			Name: "type",
		},
		{
			Kind: Varchar,
			Size: MaxInlineVarchar,
			Name: "name",
		},
		{
			Kind:     Varchar,
			Size:     MaxInlineVarchar,
			Name:     "tbl_name",
			Nullable: true,
		},
		{
			Kind: Int4,
			Size: 4,
			Name: "root_page",
		},
		{
			Kind:     Text,
			Name:     "sql",
			Nullable: true,
		},
	}
)

var (
	MainTableSQL = fmt.Sprintf(`create table "%s" (
	type int4 not null,
	name varchar(255) not null,
	tbl_name varchar(255),
	root_page int4 not null,
	sql text
);`, SchemaTableName)

	mainTableFields = fieldsFromColumns(mainTableColumns...)
)

func scanSchema(aRow Row) Schema {
	var (
		tableName string
		ddl       string
	)
	if aRow.Values[2].Valid {
		tableName = aRow.Values[2].Value.(TextPointer).String()
	}
	if aRow.Values[4].Valid {
		ddl = aRow.Values[4].Value.(TextPointer).String()
	}
	return Schema{
		Type:      SchemaType(aRow.Values[0].Value.(int32)),
		Name:      aRow.Values[1].Value.(TextPointer).String(),
		TableName: tableName,
		RootPage:  PageIndex(aRow.Values[3].Value.(int32)),
		DDL:       ddl,
	}
}

func isSystemTable(name string) bool {
	return name == SchemaTableName
}
