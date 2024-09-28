package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
)

type testCase struct {
	Name     string
	SQL      string
	Expected minisql.Statement
	Err      error
}

func TestEmpty(t *testing.T) {
	t.Parallel()

	aStatement, err := New("").Parse(context.Background())
	require.Error(t, err)
	assert.Equal(t, minisql.Statement{}, aStatement)
	assert.Equal(t, errEmptyStatementKind, err)
}
