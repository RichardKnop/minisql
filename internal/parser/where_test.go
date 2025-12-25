package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

type whereTestCase struct {
	Name     string
	SQL      string
	Expected minisql.OneOrMore
	Err      error
}

func TestParse_Where(t *testing.T) {
	t.Parallel()

	testCases := []whereTestCase{
		{
			"WHERE with integer condition",
			"WHERE b = 1",
			minisql.OneOrMore{
				{
					minisql.FieldIsEqual("b", minisql.OperandInteger, int64(1)),
				},
			},
			nil,
		},
		{
			"WHERE with float condition",
			"WHERE b = 1.5",
			minisql.OneOrMore{
				{
					minisql.FieldIsEqual("b", minisql.OperandFloat, float64(1.5)),
				},
			},
			nil,
		},
		{
			"WHERE with quoted string",
			"WHERE b = 'Foo Bar'",
			minisql.OneOrMore{
				{
					minisql.FieldIsEqual("b", minisql.OperandQuotedString, minisql.NewTextPointer([]byte("Foo Bar"))),
				},
			},
			nil,
		},
		{
			"WHERE with quoted empty string",
			"WHERE b = ''",
			minisql.OneOrMore{
				{
					minisql.FieldIsEqual("b", minisql.OperandQuotedString, minisql.NewTextPointer([]byte(""))),
				},
			},
			nil,
		},
		{
			"WHERE with field comparison",
			"WHERE a = b",
			minisql.OneOrMore{
				{
					minisql.FieldIsEqual("a", minisql.OperandField, "b"),
				},
			},
			nil,
		},
		{
			"WHERE with < works",
			"WHERE a < '1'",
			minisql.OneOrMore{
				{
					minisql.FieldIsLess("a", minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
				},
			},
			nil,
		},
		{
			"WHERE with <= works",
			"WHERE a <= '1'",
			minisql.OneOrMore{
				{
					minisql.FieldIsLessOrEqual("a", minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
				},
			},
			nil,
		},
		{
			"WHERE with > works",
			"WHERE a > 25",
			minisql.OneOrMore{
				{
					minisql.FieldIsGreater("a", minisql.OperandInteger, int64(25)),
				},
			},
			nil,
		},
		{
			"WHERE with >= works",
			"WHERE a >= 25",
			minisql.OneOrMore{
				{
					minisql.FieldIsGreaterOrEqual("a", minisql.OperandInteger, int64(25)),
				},
			},
			nil,
		},
		{
			"WHERE with != works",
			"WHERE a != '1'",
			minisql.OneOrMore{
				{
					minisql.FieldIsNotEqual("a", minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
				},
			},
			nil,
		},
		{
			"WHERE with IS NULL",
			"WHERE b IS NULL",
			minisql.OneOrMore{
				{
					minisql.FieldIsNull("b"),
				},
			},
			nil,
		},
		{
			"WHERE with IS NOT NULL",
			"WHERE b IS NOT NULL",
			minisql.OneOrMore{
				{
					minisql.FieldIsNotNull("b"),
				},
			},
			nil,
		},
		{
			"WHERE with multiple conditions",
			"WHERE a = '1' AND b = 789",
			minisql.OneOrMore{
				{
					minisql.FieldIsEqual("a", minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
					minisql.FieldIsEqual("b", minisql.OperandInteger, int64(789)),
				},
			},
			nil,
		},
		{
			"WHERE with multiple conditions using AND works",
			"WHERE a != '1' AND b = 2 and c = '3'",
			minisql.OneOrMore{
				{
					minisql.FieldIsNotEqual("a", minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
					minisql.FieldIsEqual("b", minisql.OperandInteger, int64(2)),
					minisql.FieldIsEqual("c", minisql.OperandQuotedString, minisql.NewTextPointer([]byte("3"))),
				},
			},
			nil,
		},
		{
			"WHERE with multiple conditions using OR works",
			"WHERE a != '1' OR b = 2",
			minisql.OneOrMore{
				{
					minisql.FieldIsNotEqual("a", minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
				},
				{
					minisql.FieldIsEqual("b", minisql.OperandInteger, int64(2)),
				},
			},
			nil,
		},
		{
			"WHERE with multiple conditions using both AND plus OR works",
			"WHERE a != '1' and b = 2 OR c= '3'",
			minisql.OneOrMore{
				{
					minisql.FieldIsNotEqual("a", minisql.OperandQuotedString, minisql.NewTextPointer([]byte("1"))),
					minisql.FieldIsEqual("b", minisql.OperandInteger, int64(2)),
				},
				{
					minisql.FieldIsEqual("c", minisql.OperandQuotedString, minisql.NewTextPointer([]byte("3"))),
				},
			},
			nil,
		},
		{
			"WHERE with IN condition",
			"WHERE a IN (1, 2, 3)",
			minisql.OneOrMore{
				{
					minisql.FieldIsInAny("a", int64(1), int64(2), int64(3)),
				},
			},
			nil,
		},
		{
			"WHERE with NOT IN condition",
			"WHERE a NOT IN ('b', 'c', 'd')",
			minisql.OneOrMore{
				{
					minisql.FieldIsNotInAny("a", minisql.NewTextPointer([]byte("b")), minisql.NewTextPointer([]byte("c")), minisql.NewTextPointer([]byte("d"))),
				},
			},
			nil,
		},
		{
			"WHERE with placeholders works",
			"WHERE a != ? AND b = ? and c = ?",
			minisql.OneOrMore{
				{
					minisql.FieldIsNotEqual("a", minisql.OperandPlaceholder, nil),
					minisql.FieldIsEqual("b", minisql.OperandPlaceholder, nil),
					minisql.FieldIsEqual("c", minisql.OperandPlaceholder, nil),
				},
			},
			nil,
		},
		{
			"WHERE with placeholders inside IN works",
			"WHERE a IN (?, 123, ?)",
			minisql.OneOrMore{
				{
					minisql.FieldIsInAny("a", minisql.Placeholder{}, int64(123), minisql.Placeholder{}),
				},
			},
			nil,
		},
		{
			"WHERE with placeholders inside NOT IN works",
			"WHERE a NOT IN (?, 123, ?)",
			minisql.OneOrMore{
				{
					minisql.FieldIsNotInAny("a", minisql.Placeholder{}, int64(123), minisql.Placeholder{}),
				},
			},
			nil,
		},
	}

	for _, aTestCase := range testCases {
		t.Run(aTestCase.Name, func(t *testing.T) {
			aParser := New().setSQL(aTestCase.SQL)
			aParser.step = stepWhere

			var err error
			for aParser.i < len(aParser.sql) {
				err = aParser.doParseWhere()
				if err != nil {
					break
				}
			}
			if aTestCase.Err != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, aTestCase.Err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, aTestCase.Expected, aParser.Conditions)
		})
	}
}
