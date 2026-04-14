package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_IntervalExpression(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			"INTERVAL days literal in SELECT",
			`SELECT INTERVAL '3 days' FROM "t"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "INTERVAL '3 days'",
							Expr: &minisql.Expr{Literal: minisql.Interval{Micros: 3 * 24 * 3600 * 1_000_000}},
						},
					},
				},
			},
			nil,
		},
		{
			"INTERVAL months literal",
			`SELECT INTERVAL '2 months' FROM "t"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "INTERVAL '2 months'",
							Expr: &minisql.Expr{Literal: minisql.Interval{Months: 2}},
						},
					},
				},
			},
			nil,
		},
		{
			"INTERVAL year",
			`SELECT INTERVAL '1 year' FROM "t"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "INTERVAL '1 year'",
							Expr: &minisql.Expr{Literal: minisql.Interval{Months: 12}},
						},
					},
				},
			},
			nil,
		},
		{
			"INTERVAL compound year and months",
			`SELECT INTERVAL '1 year 3 months' FROM "t"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "INTERVAL '1 year 3 months'",
							Expr: &minisql.Expr{Literal: minisql.Interval{Months: 15}},
						},
					},
				},
			},
			nil,
		},
		{
			"INTERVAL weeks converted to days",
			`SELECT INTERVAL '2 weeks' FROM "t"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "INTERVAL '14 days'",
							Expr: &minisql.Expr{Literal: minisql.Interval{Micros: 14 * 24 * 3600 * 1_000_000}},
						},
					},
				},
			},
			nil,
		},
		{
			"INTERVAL hours and minutes",
			`SELECT INTERVAL '4 hours 30 minutes' FROM "t"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "INTERVAL '4 hours 30 minutes'",
							Expr: &minisql.Expr{Literal: minisql.Interval{Micros: (4*3600 + 30*60) * 1_000_000}},
						},
					},
				},
			},
			nil,
		},
		{
			"INTERVAL negative value",
			`SELECT INTERVAL '-1 day' FROM "t"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "t",
					Fields: []minisql.Field{
						{
							Name: "INTERVAL '-1 day'",
							Expr: &minisql.Expr{Literal: minisql.Interval{Micros: -1 * 24 * 3600 * 1_000_000}},
						},
					},
				},
			},
			nil,
		},
		{
			"timestamp + INTERVAL expression",
			`SELECT created_at + INTERVAL '7 days' FROM "events"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "events",
					Fields: []minisql.Field{
						{
							Name: "created_at + INTERVAL '7 days'",
							Expr: &minisql.Expr{
								Left:  &minisql.Expr{Column: "created_at"},
								Right: &minisql.Expr{Literal: minisql.Interval{Micros: 7 * 24 * 3600 * 1_000_000}},
								Op:    minisql.ArithAdd,
							},
						},
					},
				},
			},
			nil,
		},
		{
			"timestamp - INTERVAL expression",
			`SELECT expires_at - INTERVAL '1 year' FROM "subscriptions"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "subscriptions",
					Fields: []minisql.Field{
						{
							Name: "expires_at - INTERVAL '1 year'",
							Expr: &minisql.Expr{
								Left:  &minisql.Expr{Column: "expires_at"},
								Right: &minisql.Expr{Literal: minisql.Interval{Months: 12}},
								Op:    minisql.ArithSub,
							},
						},
					},
				},
			},
			nil,
		},
		{
			"INTERVAL with AS alias",
			`SELECT created_at + INTERVAL '30 minutes' AS expires_at FROM "sessions"`,
			[]minisql.Statement{
				{
					Kind:      minisql.Select,
					TableName: "sessions",
					Fields: []minisql.Field{
						{
							Name:  "created_at + INTERVAL '30 minutes'",
							Alias: "expires_at",
							Expr: &minisql.Expr{
								Left:  &minisql.Expr{Column: "created_at"},
								Right: &minisql.Expr{Literal: minisql.Interval{Micros: 30 * 60 * 1_000_000}},
								Op:    minisql.ArithAdd,
							},
						},
					},
				},
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			statements, err := New().Parse(context.Background(), tc.SQL)
			if tc.Err != nil {
				require.Error(t, err)
				assert.Equal(t, tc.Err, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.Expected, statements)
		})
	}
}
