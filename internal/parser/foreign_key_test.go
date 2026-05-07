package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/minisql"
)

func TestParse_ForeignKey(t *testing.T) {
	t.Parallel()

	p := New()

	tests := []struct {
		name     string
		sql      string
		wantFKs  []minisql.ForeignKey
		wantErr  bool
	}{
		{
			name: "inline REFERENCES default restrict",
			sql: `CREATE TABLE orders (
				id int8 primary key autoincrement,
				user_id int8 not null references "users" (id)
			);`,
			wantFKs: []minisql.ForeignKey{
				{
					Name:         "fk__orders__users__user_id",
					Column:       "user_id",
					TargetTable:  "users",
					TargetColumn: "id",
					OnDelete:     minisql.FKActionRestrict,
					OnUpdate:     minisql.FKActionRestrict,
				},
			},
		},
		{
			name: "inline REFERENCES with ON DELETE CASCADE",
			sql: `CREATE TABLE orders (
				id int8 primary key autoincrement,
				user_id int8 not null references "users" (id) on delete cascade
			);`,
			wantFKs: []minisql.ForeignKey{
				{
					Name:         "fk__orders__users__user_id",
					Column:       "user_id",
					TargetTable:  "users",
					TargetColumn: "id",
					OnDelete:     minisql.FKActionCascade,
					OnUpdate:     minisql.FKActionRestrict,
				},
			},
		},
		{
			name: "inline REFERENCES with ON DELETE and ON UPDATE",
			sql: `CREATE TABLE orders (
				id int8 primary key autoincrement,
				user_id int8 not null references "users" (id) on delete restrict on update no action
			);`,
			wantFKs: []minisql.ForeignKey{
				{
					Name:         "fk__orders__users__user_id",
					Column:       "user_id",
					TargetTable:  "users",
					TargetColumn: "id",
					OnDelete:     minisql.FKActionRestrict,
					OnUpdate:     minisql.FKActionNoAction,
				},
			},
		},
		{
			name: "table-level FOREIGN KEY clause",
			sql: `CREATE TABLE orders (
				id int8 primary key autoincrement,
				user_id int8 not null,
				foreign key (user_id) references "users" (id)
			);`,
			wantFKs: []minisql.ForeignKey{
				{
					Name:         "fk__orders__users__user_id",
					Column:       "user_id",
					TargetTable:  "users",
					TargetColumn: "id",
					OnDelete:     minisql.FKActionRestrict,
					OnUpdate:     minisql.FKActionRestrict,
				},
			},
		},
		{
			name: "CONSTRAINT name FOREIGN KEY clause",
			sql: `CREATE TABLE orders (
				id int8 primary key autoincrement,
				user_id int8 not null,
				constraint fk_orders_users foreign key (user_id) references "users" (id)
			);`,
			wantFKs: []minisql.ForeignKey{
				{
					Name:         "fk_orders_users",
					Column:       "user_id",
					TargetTable:  "users",
					TargetColumn: "id",
					OnDelete:     minisql.FKActionRestrict,
					OnUpdate:     minisql.FKActionRestrict,
				},
			},
		},
		{
			name: "table-level FK with ON DELETE RESTRICT ON UPDATE RESTRICT",
			sql: `CREATE TABLE orders (
				id int8 primary key autoincrement,
				user_id int8 not null,
				foreign key (user_id) references "users" (id) on delete restrict on update restrict
			);`,
			wantFKs: []minisql.ForeignKey{
				{
					Name:         "fk__orders__users__user_id",
					Column:       "user_id",
					TargetTable:  "users",
					TargetColumn: "id",
					OnDelete:     minisql.FKActionRestrict,
					OnUpdate:     minisql.FKActionRestrict,
				},
			},
		},
		{
			name: "multiple FKs on same table",
			sql: `CREATE TABLE order_items (
				id int8 primary key autoincrement,
				order_id int8 not null references "orders" (id),
				product_id int8 not null references "products" (id)
			);`,
			wantFKs: []minisql.ForeignKey{
				{
					Name:         "fk__order_items__orders__order_id",
					Column:       "order_id",
					TargetTable:  "orders",
					TargetColumn: "id",
					OnDelete:     minisql.FKActionRestrict,
					OnUpdate:     minisql.FKActionRestrict,
				},
				{
					Name:         "fk__order_items__products__product_id",
					Column:       "product_id",
					TargetTable:  "products",
					TargetColumn: "id",
					OnDelete:     minisql.FKActionRestrict,
					OnUpdate:     minisql.FKActionRestrict,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := p.Parse(context.Background(), tt.sql)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.Equal(t, tt.wantFKs, stmts[0].ForeignKeys)
		})
	}
}

func TestParse_ForeignKey_DDLRoundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "inline REFERENCES without actions",
			sql: `CREATE TABLE orders (
				id int8 primary key autoincrement,
				user_id int8 not null references "users" (id)
			);`,
		},
		{
			name: "table-level FOREIGN KEY without actions",
			sql: `CREATE TABLE orders (
				id int8 primary key autoincrement,
				user_id int8 not null,
				foreign key (user_id) references "users" (id)
			);`,
		},
		{
			name: "CONSTRAINT name FOREIGN KEY without actions",
			sql: `CREATE TABLE orders (
				id int8 primary key autoincrement,
				user_id int8 not null,
				constraint fk_orders_users foreign key (user_id) references "users" (id)
			);`,
		},
	}

	p := New()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := p.Parse(context.Background(), tt.sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			require.Len(t, stmts[0].ForeignKeys, 1)

			fk := stmts[0].ForeignKeys[0]
			assert.Equal(t, minisql.FKActionRestrict, fk.OnDelete, "unspecified ON DELETE should default to RESTRICT")
			assert.Equal(t, minisql.FKActionRestrict, fk.OnUpdate, "unspecified ON UPDATE should default to RESTRICT")

			// The emitted DDL must always include explicit on delete/on update
			// clauses so that re-parsing after a database reopen produces the
			// same FK definition.
			ddl := stmts[0].DDL()
			assert.Contains(t, ddl, "on delete restrict on update restrict",
				"DDL must serialise default RESTRICT actions explicitly")

			// Re-parse the generated DDL to ensure the FK survives a round-trip.
			stmts2, err := p.Parse(context.Background(), ddl)
			require.NoError(t, err)
			require.Len(t, stmts2, 1)
			assert.Equal(t, stmts[0].ForeignKeys, stmts2[0].ForeignKeys)
		})
	}
}
