package minisqltest

import (
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
)

var (
	gen = NewDataGen(time.Now().Unix())

	testColumns = []minisql.Column{
		{
			Kind: minisql.Int8,
			Size: 8,
			Name: "id",
		},
		{
			Kind: minisql.Varchar,
			Size: 255,
			Name: "email",
		},
		{
			Kind: minisql.Int4,
			Size: 4,
			Name: "age",
		},
	}
)

type DataGen struct {
	*gofakeit.Faker
}

func NewDataGen(seed int64) *DataGen {
	g := DataGen{
		Faker: gofakeit.New(seed),
	}

	return &g
}

func (g *DataGen) Rows(number int) []minisql.Row {
	rows := make([]minisql.Row, 0, number)
	for i := 0; i < number; i++ {
		rows = append(rows, g.Row())
	}
	return rows
}

func (g *DataGen) Row() minisql.Row {
	return minisql.Row{
		Columns: testColumns,
		Values: []any{
			g.Int64(),
			g.Email(),
			int32(g.IntRange(18, 100)),
		},
	}
}
