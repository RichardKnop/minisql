package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartialIndexImplied(t *testing.T) {
	t.Parallel()

	statusField := Field{Name: "status"}
	activeValue := NewTextPointer([]byte("active"))

	statusActive := Condition{
		Operand1: Operand{Type: OperandField, Value: statusField},
		Operand2: Operand{Type: OperandQuotedString, Value: activeValue},
		Operator: Eq,
	}

	archivedField := Field{Name: "archived"}
	archivedFalse := Condition{
		Operand1: Operand{Type: OperandField, Value: archivedField},
		Operand2: Operand{Type: OperandBoolean, Value: false},
		Operator: Eq,
	}

	t.Run("full index (nil WhereCond) always implied", func(t *testing.T) {
		t.Parallel()
		assert.True(t, partialIndexImplied(nil, nil))
		assert.True(t, partialIndexImplied(nil, Conditions{statusActive}))
	})

	t.Run("empty WhereCond always implied", func(t *testing.T) {
		t.Parallel()
		assert.True(t, partialIndexImplied(OneOrMore{}, Conditions{statusActive}))
	})

	t.Run("single-term index cond implied when term appears in query", func(t *testing.T) {
		t.Parallel()
		indexWhere := OneOrMore{{statusActive}}
		queryGroup := Conditions{statusActive, archivedFalse}
		assert.True(t, partialIndexImplied(indexWhere, queryGroup))
	})

	t.Run("single-term index cond not implied when term absent from query", func(t *testing.T) {
		t.Parallel()
		indexWhere := OneOrMore{{statusActive}}
		queryGroup := Conditions{archivedFalse}
		assert.False(t, partialIndexImplied(indexWhere, queryGroup))
	})

	t.Run("multi-term index AND cond implied when all terms in query", func(t *testing.T) {
		t.Parallel()
		indexWhere := OneOrMore{{statusActive, archivedFalse}}
		queryGroup := Conditions{archivedFalse, statusActive}
		assert.True(t, partialIndexImplied(indexWhere, queryGroup))
	})

	t.Run("multi-term index AND cond not implied when one term missing", func(t *testing.T) {
		t.Parallel()
		indexWhere := OneOrMore{{statusActive, archivedFalse}}
		queryGroup := Conditions{statusActive}
		assert.False(t, partialIndexImplied(indexWhere, queryGroup))
	})

	t.Run("multi-group (OR) index cond is conservatively not implied", func(t *testing.T) {
		t.Parallel()
		indexWhere := OneOrMore{{statusActive}, {archivedFalse}}
		queryGroup := Conditions{statusActive, archivedFalse}
		assert.False(t, partialIndexImplied(indexWhere, queryGroup))
	})

	t.Run("operator mismatch means condition not implied", func(t *testing.T) {
		t.Parallel()
		statusNotActive := Condition{
			Operand1: Operand{Type: OperandField, Value: statusField},
			Operand2: Operand{Type: OperandQuotedString, Value: activeValue},
			Operator: Ne,
		}
		indexWhere := OneOrMore{{statusActive}}
		queryGroup := Conditions{statusNotActive}
		assert.False(t, partialIndexImplied(indexWhere, queryGroup))
	})

	t.Run("value mismatch means condition not implied", func(t *testing.T) {
		t.Parallel()
		statusInactive := Condition{
			Operand1: Operand{Type: OperandField, Value: statusField},
			Operand2: Operand{Type: OperandQuotedString, Value: NewTextPointer([]byte("inactive"))},
			Operator: Eq,
		}
		indexWhere := OneOrMore{{statusActive}}
		queryGroup := Conditions{statusInactive}
		assert.False(t, partialIndexImplied(indexWhere, queryGroup))
	})
}
