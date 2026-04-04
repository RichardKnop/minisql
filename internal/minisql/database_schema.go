package minisql

import (
	"fmt"
)

const (
	// SchemaTableName is the internal table used to store schema metadata.
	SchemaTableName = "minisql_schema"
	// StatsTableName ...
	StatsTableName = "minisql_stats"
	// MaxColumns ...
	MaxColumns = 64
	// RootPageConfigSize is the number of bytes reserved for the root page config header.
	RootPageConfigSize = 100
)

// SchemaType identifies the kind of object recorded in the schema table.
type SchemaType int

// SchemaType constants identify the kind of object recorded in the schema table.
const (
	// SchemaTable identifies a user table entry in the schema.
	SchemaTable SchemaType = iota + 1
	SchemaPrimaryKey
	SchemaUniqueIndex
	SchemaSecondaryIndex
)

// Schema represents a single row in the internal schema metadata table.
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

	// MainTableSQL is the DDL used to create the internal schema metadata table.
	MainTableSQL = fmt.Sprintf(`create table "%s" (
	type int4 not null,
	name varchar(255) not null,
	tbl_name varchar(255),
	root_page int4 not null,
	sql text
);`, SchemaTableName)

	mainTableFields = fieldsFromColumns(mainTableColumns...)
)

func scanSchema(row Row) Schema {
	var (
		tableName string
		ddl       string
	)
	if row.Values[2].Valid {
		tableName = row.Values[2].Value.(TextPointer).String()
	}
	if row.Values[4].Valid {
		ddl = row.Values[4].Value.(TextPointer).String()
	}
	return Schema{
		Type:      SchemaType(row.Values[0].Value.(int32)),
		Name:      row.Values[1].Value.(TextPointer).String(),
		TableName: tableName,
		RootPage:  PageIndex(row.Values[3].Value.(int32)),
		DDL:       ddl,
	}
}

func isSystemTable(name string) bool {
	return name == SchemaTableName || name == StatsTableName
}
