package minisql

import (
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

var (
	gen = NewDataGen(time.Now().Unix())

	testColumns = []Column{
		{
			Kind: Int8,
			Size: 8,
			Name: "id",
		},
		{
			Kind: Varchar,
			Size: 255,
			Name: "email",
		},
		{
			Kind: Int4,
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

func (g *DataGen) Row() Row {
	return Row{
		Columns: []Column{
			{
				Kind: Int8,
				Size: 8,
				Name: "id",
			},
			{
				Kind: Varchar,
				Size: 255,
				Name: "email",
			},
			{
				Kind: Int4,
				Size: 4,
				Name: "age",
			},
		},
		Values: []any{
			g.Int64(),
			g.Email(),
			int32(g.IntRange(18, 100)),
		},
	}
}
