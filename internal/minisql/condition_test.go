package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsValidCondition(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		condition Condition
		name      string
		isValid   bool
	}{
		{
			name: "valid condition with integer operand",
			condition: Condition{
				Operand1: Operand{
					Type:  OperandField,
					Value: Field{Name: "a"},
				},
				Operator: Eq,
				Operand2: Operand{
					Type:  OperandInteger,
					Value: int64(10),
				},
			},
			isValid: true,
		},
		{
			name: "invalid condition with missing operand",
			condition: Condition{
				Operand1: Operand{
					Type:  OperandField,
					Value: Field{Name: "a"},
				},
				Operator: Eq,
			},
			isValid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isValid := IsValidCondition(tc.condition)
			assert.Equal(t, tc.isValid, isValid)
		})
	}
}

func TestFieldIsEqual(t *testing.T) {
	t.Parallel()

	t.Run("string field with quoted string value", func(t *testing.T) {
		condition := FieldIsEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("john@example.com")))

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "email"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandQuotedString,
				Value: NewTextPointer([]byte("john@example.com")),
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("boolean field with boolean value", func(t *testing.T) {
		condition := FieldIsEqual(Field{Name: "verified"}, OperandBoolean, true)

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "verified"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandBoolean,
				Value: true,
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("integer field with integer value", func(t *testing.T) {
		condition := FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(25))

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "id"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandInteger,
				Value: int64(25),
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("float field with float value", func(t *testing.T) {
		condition := FieldIsEqual(Field{Name: "score"}, OperandFloat, 95.5)

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "score"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandFloat,
				Value: 95.5,
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("field with null value", func(t *testing.T) {
		condition := FieldIsEqual(Field{Name: "description"}, OperandNull, nil)

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "description"},
			},
			Operator: Eq,
			Operand2: Operand{
				Type:  OperandNull,
				Value: nil,
			},
		}

		assert.Equal(t, expected, condition)
	})
}

func TestFieldIsNotEqual(t *testing.T) {
	t.Parallel()

	t.Run("string field with quoted string value", func(t *testing.T) {
		condition := FieldIsNotEqual(Field{Name: "email"}, OperandQuotedString, NewTextPointer([]byte("john@example.com")))

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "email"},
			},
			Operator: Ne,
			Operand2: Operand{
				Type:  OperandQuotedString,
				Value: NewTextPointer([]byte("john@example.com")),
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("boolean field with boolean value", func(t *testing.T) {
		condition := FieldIsNotEqual(Field{Name: "verified"}, OperandBoolean, true)

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "verified"},
			},
			Operator: Ne,
			Operand2: Operand{
				Type:  OperandBoolean,
				Value: true,
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("integer field with integer value", func(t *testing.T) {
		condition := FieldIsNotEqual(Field{Name: "id"}, OperandInteger, int64(25))

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "id"},
			},
			Operator: Ne,
			Operand2: Operand{
				Type:  OperandInteger,
				Value: int64(25),
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("float field with float value", func(t *testing.T) {
		condition := FieldIsNotEqual(Field{Name: "score"}, OperandFloat, 95.5)

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "score"},
			},
			Operator: Ne,
			Operand2: Operand{
				Type:  OperandFloat,
				Value: 95.5,
			},
		}

		assert.Equal(t, expected, condition)
	})

	t.Run("field with null value", func(t *testing.T) {
		condition := FieldIsNotEqual(Field{Name: "description"}, OperandNull, nil)

		expected := Condition{
			Operand1: Operand{
				Type:  OperandField,
				Value: Field{Name: "description"},
			},
			Operator: Ne,
			Operand2: Operand{
				Type:  OperandNull,
				Value: nil,
			},
		}

		assert.Equal(t, expected, condition)
	})
}

func TestCompareBoolean(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		operator Operator
		a        bool
		b        bool
		want     bool
		wantErr  bool
	}{
		// Equality
		{
			name:     "true equals true",
			a:        true,
			b:        true,
			operator: Eq,
			want:     true,
		},
		{
			name:     "false equals false",
			a:        false,
			b:        false,
			operator: Eq,
			want:     true,
		},
		{
			name:     "true not equals false",
			a:        true,
			b:        false,
			operator: Eq,
			want:     false,
		},
		{
			name:     "false not equals true",
			a:        false,
			b:        true,
			operator: Eq,
			want:     false,
		},

		// Inequality
		{
			name:     "true != false",
			a:        true,
			b:        false,
			operator: Ne,
			want:     true,
		},
		{
			name:     "false != true",
			a:        false,
			b:        true,
			operator: Ne,
			want:     true,
		},
		{
			name:     "true != true is false",
			a:        true,
			b:        true,
			operator: Ne,
			want:     false,
		},
		{
			name:     "false != false is false",
			a:        false,
			b:        false,
			operator: Ne,
			want:     false,
		},

		// Unsupported operators
		{
			name:     "boolean > operator not supported",
			a:        true,
			b:        false,
			operator: Gt,
			wantErr:  true,
		},
		{
			name:     "boolean < operator not supported",
			a:        true,
			b:        false,
			operator: Lt,
			wantErr:  true,
		},
		{
			name:     "boolean >= operator not supported",
			a:        true,
			b:        false,
			operator: Gte,
			wantErr:  true,
		},
		{
			name:     "boolean <= operator not supported",
			a:        true,
			b:        false,
			operator: Lte,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compareBoolean(tt.a, tt.b, tt.operator)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCompareInt4(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		a        int64
		b        int64
		operator Operator
		want     bool
		wantErr  bool
	}{
		// Equality
		{
			name:     "0 equals 0",
			a:        0,
			b:        0,
			operator: Eq,
			want:     true,
		},
		{
			name:     "42 equals 42",
			a:        42,
			b:        42,
			operator: Eq,
			want:     true,
		},
		{
			name:     "negative equals negative",
			a:        -100,
			b:        -100,
			operator: Eq,
			want:     true,
		},
		{
			name:     "5 not equals 10",
			a:        5,
			b:        10,
			operator: Eq,
			want:     false,
		},

		// Inequality
		{
			name:     "5 != 10",
			a:        5,
			b:        10,
			operator: Ne,
			want:     true,
		},
		{
			name:     "42 != 42 is false",
			a:        42,
			b:        42,
			operator: Ne,
			want:     false,
		},

		// Greater than
		{
			name:     "10 > 5",
			a:        10,
			b:        5,
			operator: Gt,
			want:     true,
		},
		{
			name:     "5 > 10 is false",
			a:        5,
			b:        10,
			operator: Gt,
			want:     false,
		},
		{
			name:     "5 > 5 is false",
			a:        5,
			b:        5,
			operator: Gt,
			want:     false,
		},
		{
			name:     "0 > -1",
			a:        0,
			b:        -1,
			operator: Gt,
			want:     true,
		},
		{
			name:     "-1 > -2",
			a:        -1,
			b:        -2,
			operator: Gt,
			want:     true,
		},

		// Greater than or equal
		{
			name:     "10 >= 5",
			a:        10,
			b:        5,
			operator: Gte,
			want:     true,
		},
		{
			name:     "5 >= 5",
			a:        5,
			b:        5,
			operator: Gte,
			want:     true,
		},
		{
			name:     "5 >= 10 is false",
			a:        5,
			b:        10,
			operator: Gte,
			want:     false,
		},

		// Less than
		{
			name:     "5 < 10",
			a:        5,
			b:        10,
			operator: Lt,
			want:     true,
		},
		{
			name:     "10 < 5 is false",
			a:        10,
			b:        5,
			operator: Lt,
			want:     false,
		},
		{
			name:     "5 < 5 is false",
			a:        5,
			b:        5,
			operator: Lt,
			want:     false,
		},
		{
			name:     "-1 < 0",
			a:        -1,
			b:        0,
			operator: Lt,
			want:     true,
		},

		// Less than or equal
		{
			name:     "5 <= 10",
			a:        5,
			b:        10,
			operator: Lte,
			want:     true,
		},
		{
			name:     "5 <= 5",
			a:        5,
			b:        5,
			operator: Lte,
			want:     true,
		},
		{
			name:     "10 <= 5 is false",
			a:        10,
			b:        5,
			operator: Lte,
			want:     false,
		},

		// Edge cases
		{
			name:     "max int32",
			a:        2147483647,
			b:        2147483647,
			operator: Eq,
			want:     true,
		},
		{
			name:     "min int32",
			a:        -2147483648,
			b:        -2147483648,
			operator: Eq,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compareInt4(tt.a, tt.b, tt.operator)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCompareInt8(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		a        int64
		b        int64
		operator Operator
		want     bool
		wantErr  bool
	}{
		// Equality
		{
			name:     "0 equals 0",
			a:        0,
			b:        0,
			operator: Eq,
			want:     true,
		},
		{
			name:     "large number equals",
			a:        9223372036854775807,
			b:        9223372036854775807,
			operator: Eq,
			want:     true,
		},
		{
			name:     "negative equals negative",
			a:        -1000000000000,
			b:        -1000000000000,
			operator: Eq,
			want:     true,
		},

		// Inequality
		{
			name:     "different values not equal",
			a:        100,
			b:        200,
			operator: Ne,
			want:     true,
		},

		// Greater than
		{
			name:     "1000000 > 1",
			a:        1000000,
			b:        1,
			operator: Gt,
			want:     true,
		},
		{
			name:     "large > small",
			a:        9223372036854775806,
			b:        1,
			operator: Gt,
			want:     true,
		},

		// Greater than or equal
		{
			name:     "equal values >=",
			a:        1000000000,
			b:        1000000000,
			operator: Gte,
			want:     true,
		},

		// Less than
		{
			name:     "1 < 1000000",
			a:        1,
			b:        1000000,
			operator: Lt,
			want:     true,
		},
		{
			name:     "negative < positive",
			a:        -1000000000000,
			b:        1,
			operator: Lt,
			want:     true,
		},

		// Less than or equal
		{
			name:     "small <= large",
			a:        100,
			b:        1000000,
			operator: Lte,
			want:     true,
		},

		// Edge cases
		{
			name:     "max int64",
			a:        9223372036854775807,
			b:        9223372036854775807,
			operator: Eq,
			want:     true,
		},
		{
			name:     "min int64",
			a:        -9223372036854775808,
			b:        -9223372036854775808,
			operator: Eq,
			want:     true,
		},
		{
			name:     "min < max",
			a:        -9223372036854775808,
			b:        9223372036854775807,
			operator: Lt,
			want:     true,
		},

		// Unknown operator
		{
			name:     "unknown operator",
			a:        5,
			b:        10,
			operator: Operator(999),
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compareInt8(tt.a, tt.b, tt.operator)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCompareReal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		a        float64
		b        float64
		operator Operator
		want     bool
		wantErr  bool
	}{
		// Equality
		{
			name:     "0.0 equals 0.0",
			a:        0.0,
			b:        0.0,
			operator: Eq,
			want:     true,
		},
		{
			name:     "42.69 equals 42.69",
			a:        42.69,
			b:        42.69,
			operator: Eq,
			want:     true,
		},
		{
			name:     "negative equals negative",
			a:        -100.15,
			b:        -100.15,
			operator: Eq,
			want:     true,
		},
		{
			name:     "5.0 not equals 10.5",
			a:        5.0,
			b:        10.5,
			operator: Eq,
			want:     false,
		},

		// Inequality
		{
			name:     "5.0 != 10.5",
			a:        5.0,
			b:        10.5,
			operator: Ne,
			want:     true,
		},
		{
			name:     "42.69 != 42.69 is false",
			a:        42.69,
			b:        42.69,
			operator: Ne,
			want:     false,
		},

		// Greater than
		{
			name:     "10.5 > 5.0",
			a:        10.5,
			b:        5.0,
			operator: Gt,
			want:     true,
		},
		{
			name:     "5.0 > 10.5 is false",
			a:        5.0,
			b:        10.5,
			operator: Gt,
			want:     false,
		},
		{
			name:     "5.0 > 5.0 is false",
			a:        5.0,
			b:        5.0,
			operator: Gt,
			want:     false,
		},
		{
			name:     "0.0 > -1.0",
			a:        0.0,
			b:        -1.0,
			operator: Gt,
			want:     true,
		},
		{
			name:     "-1.0 > -2.0",
			a:        -1.0,
			b:        -2.2,
			operator: Gt,
			want:     true,
		},

		// Greater than or equal
		{
			name:     "10.5 >= 5.0",
			a:        10.0,
			b:        5.5,
			operator: Gte,
			want:     true,
		},
		{
			name:     "5.0 >= 5.0",
			a:        5.0,
			b:        5.0,
			operator: Gte,
			want:     true,
		},
		{
			name:     "5.0 >= 10.5 is false",
			a:        5.0,
			b:        10.5,
			operator: Gte,
			want:     false,
		},

		// Less than
		{
			name:     "5.0 < 10.5",
			a:        5.0,
			b:        10.5,
			operator: Lt,
			want:     true,
		},
		{
			name:     "10.5 < 5.0 is false",
			a:        10.5,
			b:        5.0,
			operator: Lt,
			want:     false,
		},
		{
			name:     "5.0 < 5.0 is false",
			a:        5.0,
			b:        5.0,
			operator: Lt,
			want:     false,
		},
		{
			name:     "-1.0 < 0.0",
			a:        -1.0,
			b:        0.0,
			operator: Lt,
			want:     true,
		},

		// Less than or equal
		{
			name:     "5.0 <= 10.5",
			a:        5.0,
			b:        10.5,
			operator: Lte,
			want:     true,
		},
		{
			name:     "5.0 <= 5.0",
			a:        5.0,
			b:        5.0,
			operator: Lte,
			want:     true,
		},
		{
			name:     "10.5 <= 5.0 is false",
			a:        10.5,
			b:        5.0,
			operator: Lte,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compareReal(tt.a, tt.b, tt.operator)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCompareDouble(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		a        float64
		b        float64
		operator Operator
		want     bool
		wantErr  bool
	}{
		// Equality
		{
			name:     "0.0 equals 0.0",
			a:        0.0,
			b:        0.0,
			operator: Eq,
			want:     true,
		},
		{
			name:     "42.69 equals 42.69",
			a:        42.69,
			b:        42.69,
			operator: Eq,
			want:     true,
		},
		{
			name:     "negative equals negative",
			a:        -100.15,
			b:        -100.15,
			operator: Eq,
			want:     true,
		},
		{
			name:     "5.0 not equals 10.5",
			a:        5.0,
			b:        10.5,
			operator: Eq,
			want:     false,
		},

		// Inequality
		{
			name:     "5.0 != 10.5",
			a:        5.0,
			b:        10.5,
			operator: Ne,
			want:     true,
		},
		{
			name:     "42.69 != 42.69 is false",
			a:        42.69,
			b:        42.69,
			operator: Ne,
			want:     false,
		},

		// Greater than
		{
			name:     "10.5 > 5.0",
			a:        10.5,
			b:        5.0,
			operator: Gt,
			want:     true,
		},
		{
			name:     "5.0 > 10.5 is false",
			a:        5.0,
			b:        10.5,
			operator: Gt,
			want:     false,
		},
		{
			name:     "5.0 > 5.0 is false",
			a:        5.0,
			b:        5.0,
			operator: Gt,
			want:     false,
		},
		{
			name:     "0.0 > -1.0",
			a:        0.0,
			b:        -1.0,
			operator: Gt,
			want:     true,
		},
		{
			name:     "-1.0 > -2.0",
			a:        -1.0,
			b:        -2.2,
			operator: Gt,
			want:     true,
		},

		// Greater than or equal
		{
			name:     "10.5 >= 5.0",
			a:        10.0,
			b:        5.5,
			operator: Gte,
			want:     true,
		},
		{
			name:     "5.0 >= 5.0",
			a:        5.0,
			b:        5.0,
			operator: Gte,
			want:     true,
		},
		{
			name:     "5.0 >= 10.5 is false",
			a:        5.0,
			b:        10.5,
			operator: Gte,
			want:     false,
		},

		// Less than
		{
			name:     "5.0 < 10.5",
			a:        5.0,
			b:        10.5,
			operator: Lt,
			want:     true,
		},
		{
			name:     "10.5 < 5.0 is false",
			a:        10.5,
			b:        5.0,
			operator: Lt,
			want:     false,
		},
		{
			name:     "5.0 < 5.0 is false",
			a:        5.0,
			b:        5.0,
			operator: Lt,
			want:     false,
		},
		{
			name:     "-1.0 < 0.0",
			a:        -1.0,
			b:        0.0,
			operator: Lt,
			want:     true,
		},

		// Less than or equal
		{
			name:     "5.0 <= 10.5",
			a:        5.0,
			b:        10.5,
			operator: Lte,
			want:     true,
		},
		{
			name:     "5.0 <= 5.0",
			a:        5.0,
			b:        5.0,
			operator: Lte,
			want:     true,
		},
		{
			name:     "10.5 <= 5.0 is false",
			a:        10.5,
			b:        5.0,
			operator: Lte,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compareDouble(tt.a, tt.b, tt.operator)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCompareText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		a        string
		b        string
		operator Operator
		want     bool
		wantErr  bool
	}{
		// Equal
		{
			name:     "equal strings",
			a:        "foobar",
			b:        "foobar",
			operator: Eq,
			want:     true,
		},
		{
			name:     "different strings not equal",
			a:        "hello",
			b:        "world",
			operator: Eq,
			want:     false,
		},
		{
			name:     "empty strings equal",
			a:        "",
			b:        "",
			operator: Eq,
			want:     true,
		},

		// Not equal
		{
			name:     "not equal strings",
			a:        "foo",
			b:        "bar",
			operator: Ne,
			want:     true,
		},

		// Greater than
		{
			name:     "a > b",
			a:        "zebra",
			b:        "apple",
			operator: Gt,
			want:     true,
		},
		{
			name:     "a > b is false",
			a:        "apple",
			b:        "zebra",
			operator: Gt,
			want:     false,
		},
		{
			name:     "equal strings not >",
			a:        "same",
			b:        "same",
			operator: Gt,
			want:     false,
		},

		// Greater than or equal
		{
			name:     "a >= b",
			a:        "banana",
			b:        "apple",
			operator: Gte,
			want:     true,
		},
		{
			name:     "a >= b is false",
			a:        "apple",
			b:        "banana",
			operator: Gte,
			want:     false,
		},
		{
			name:     "equal strings >=",
			a:        "same",
			b:        "same",
			operator: Gte,
			want:     true,
		},

		// Less than
		{
			name:     "a < b",
			a:        "apple",
			b:        "banana",
			operator: Lt,
			want:     true,
		},
		{
			name:     "a < b is false",
			a:        "zebra",
			b:        "apple",
			operator: Lt,
			want:     false,
		},
		{
			name:     "equal strings not <",
			a:        "same",
			b:        "same",
			operator: Lt,
			want:     false,
		},

		// Less than or equal
		{
			name:     "a <= b",
			a:        "apple",
			b:        "banana",
			operator: Lte,
			want:     true,
		},
		{
			name:     "a <= b is false",
			a:        "banana",
			b:        "apple",
			operator: Lte,
			want:     false,
		},
		{
			name:     "equal strings <=",
			a:        "same",
			b:        "same",
			operator: Lte,
			want:     true,
		},

		// Case sensitivity
		{
			name:     "case sensitive comparison",
			a:        "Hello",
			b:        "hello",
			operator: Eq,
			want:     false,
		},
		{
			name:     "uppercase < lowercase (ASCII)",
			a:        "A",
			b:        "a",
			operator: Lt,
			want:     true,
		},

		// Special characters
		{
			name:     "strings with spaces",
			a:        "hello world",
			b:        "hello world",
			operator: Eq,
			want:     true,
		},
		{
			name:     "strings with numbers",
			a:        "test123/*-",
			b:        "test123/*-",
			operator: Eq,
			want:     true,
		},

		// Unicode
		{
			name:     "unicode strings equal",
			a:        "café",
			b:        "café",
			operator: Eq,
			want:     true,
		},
		{
			name:     "emoji comparison",
			a:        "hello 👋",
			b:        "hello 👋",
			operator: Eq,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compareText(
				NewTextPointer([]byte(tt.a)),
				NewTextPointer([]byte(tt.b)),
				tt.operator,
			)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsBetweenInt4(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   int64
		low     int64
		high    int64
		want    bool
		wantErr bool
	}{
		{"value in range", 5, 1, 10, true, false},
		{"value at lower bound", 1, 1, 10, true, false},
		{"value at upper bound", 10, 1, 10, true, false},
		{"value below lower bound", 0, 1, 10, false, false},
		{"value above upper bound", 11, 1, 10, false, false},
		{"negative range", -5, -10, -1, true, false},
		{"single value range", 5, 5, 5, true, false},
		{"single value range no match", 4, 5, 5, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isBetweenInt4(tt.value, tt.low, tt.high)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsBetweenInt8(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value int64
		low   int64
		high  int64
		want  bool
	}{
		{"value in range", 500, 100, 1000, true},
		{"value at lower bound", 100, 100, 1000, true},
		{"value at upper bound", 1000, 100, 1000, true},
		{"value below", 99, 100, 1000, false},
		{"value above", 1001, 100, 1000, false},
		{"large range", 9223372036854775806, 0, 9223372036854775807, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isBetweenInt8(tt.value, tt.low, tt.high)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsBetweenText(t *testing.T) {
	t.Parallel()

	mkPtr := func(s string) TextPointer { return NewTextPointer([]byte(s)) }

	tests := []struct {
		name  string
		value string
		low   string
		high  string
		want  bool
	}{
		{"value in range", "mango", "apple", "zebra", true},
		{"value at lower bound", "apple", "apple", "zebra", true},
		{"value at upper bound", "zebra", "apple", "zebra", true},
		{"value below lower", "aardvark", "apple", "zebra", false},
		{"value above upper", "zoo", "apple", "zebra", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isBetweenText(mkPtr(tt.value), mkPtr(tt.low), mkPtr(tt.high))
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsBetweenReal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   float64
		low     float64
		high    float64
		want    bool
		wantErr bool
	}{
		{"value in range", 5.5, 1.0, 10.0, true, false},
		{"value at lower bound", 1.0, 1.0, 10.0, true, false},
		{"value at upper bound", 10.0, 1.0, 10.0, true, false},
		{"value below lower bound", 0.9, 1.0, 10.0, false, false},
		{"value above upper bound", 10.1, 1.0, 10.0, false, false},
		{"negative range", -5.0, -10.0, -1.0, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isBetweenReal(tt.value, tt.low, tt.high)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsBetweenDouble(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value float64
		low   float64
		high  float64
		want  bool
	}{
		{"value in range", 5.5, 1.0, 10.0, true},
		{"value at lower bound", 1.0, 1.0, 10.0, true},
		{"value at upper bound", 10.0, 1.0, 10.0, true},
		{"value below lower bound", 0.99, 1.0, 10.0, false},
		{"value above upper bound", 10.01, 1.0, 10.0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isBetweenDouble(tt.value, tt.low, tt.high)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsBetweenTimestamp(t *testing.T) {
	t.Parallel()

	t1 := MustParseTimestampMicros("2020-01-01 00:00:00")
	t2 := MustParseTimestampMicros("2021-06-15 12:00:00")
	t3 := MustParseTimestampMicros("2022-12-31 23:59:59")

	tests := []struct {
		name  string
		value TimestampMicros
		low   TimestampMicros
		high  TimestampMicros
		want  bool
	}{
		{"value in range", t2, t1, t3, true},
		{"value at lower bound", t1, t1, t3, true},
		{"value at upper bound", t3, t1, t3, true},
		{"value below lower bound", t1, t2, t3, false},
		{"value above upper bound", t3, t1, t2, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isBetweenTimestamp(tt.value, tt.low, tt.high)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsInListInt4(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value   any
		list    any
		name    string
		want    bool
		wantErr bool
	}{
		{name: "found in list", value: int64(5), list: []any{int64(1), int64(5), int64(10)}, want: true},
		{name: "not found in list", value: int64(7), list: []any{int64(1), int64(5), int64(10)}, want: false},
		{name: "empty list", value: int64(5), list: []any{}, want: false},
		{name: "single element match", value: int64(42), list: []any{int64(42)}, want: true},
		{name: "negative value found", value: int64(-5), list: []any{int64(-10), int64(-5), int64(0)}, want: true},
		{name: "invalid value type", value: "not an int", list: []any{int64(1)}, wantErr: true},
		{name: "invalid list type", value: int64(1), list: "not a list", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isInListInt4(tt.value, tt.list)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsInListReal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value   any
		list    any
		name    string
		want    bool
		wantErr bool
	}{
		{name: "found in list", value: float64(5.5), list: []any{float64(1.1), float64(5.5), float64(10.0)}, want: true},
		{name: "not found in list", value: float64(7.7), list: []any{float64(1.1), float64(5.5), float64(10.0)}, want: false},
		{name: "empty list", value: float64(5.5), list: []any{}, want: false},
		{name: "invalid value type", value: "not a float", list: []any{float64(1.0)}, wantErr: true},
		{name: "invalid list type", value: float64(1.0), list: "not a list", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isInListReal(tt.value, tt.list)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsInListDouble(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value   any
		list    any
		name    string
		want    bool
		wantErr bool
	}{
		{name: "found in list", value: float64(5.5), list: []any{float64(1.1), float64(5.5), float64(10.0)}, want: true},
		{name: "not found in list", value: float64(7.7), list: []any{float64(1.1), float64(5.5), float64(10.0)}, want: false},
		{name: "empty list", value: float64(5.5), list: []any{}, want: false},
		{name: "invalid value type", value: "not a float", list: []any{float64(1.0)}, wantErr: true},
		{name: "invalid list type", value: float64(1.0), list: "not a list", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isInListDouble(tt.value, tt.list)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsInListTimestamp(t *testing.T) {
	t.Parallel()

	t1 := MustParseTimestampMicros("2020-01-01 00:00:00")
	t2 := MustParseTimestampMicros("2021-06-15 12:00:00")
	t3 := MustParseTimestampMicros("2022-12-31 23:59:59")

	tests := []struct {
		value   any
		list    any
		name    string
		want    bool
		wantErr bool
	}{
		{name: "found in list", value: t2, list: []any{t1, t2, t3}, want: true},
		{name: "not found in list", value: t3, list: []any{t1, t2}, want: false},
		{name: "empty list", value: t1, list: []any{}, want: false},
		{name: "invalid value type", value: "not a time", list: []any{t1}, wantErr: true},
		{name: "invalid list type", value: t1, list: "not a list", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isInListTimestamp(tt.value, tt.list)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCompareTimestamp(t *testing.T) {
	t.Parallel()

	t1 := MustParseTimestampMicros("2020-01-01 00:00:00")
	t2 := MustParseTimestampMicros("2021-06-15 12:00:00")
	t3 := MustParseTimestampMicros("2021-06-15 12:00:00")

	tests := []struct {
		a        any
		b        any
		name     string
		operator Operator
		want     bool
		wantErr  bool
	}{
		{name: "equal timestamps", a: t2, b: t3, operator: Eq, want: true},
		{name: "not equal", a: t1, b: t2, operator: Eq, want: false},
		{name: "not equal op", a: t1, b: t2, operator: Ne, want: true},
		{name: "greater than", a: t2, b: t1, operator: Gt, want: true},
		{name: "less than", a: t1, b: t2, operator: Lt, want: true},
		{name: "greater or equal same", a: t2, b: t3, operator: Gte, want: true},
		{name: "less or equal same", a: t2, b: t3, operator: Lte, want: true},
		{name: "unknown operator", a: t1, b: t2, operator: Operator(999), wantErr: true},
		{name: "invalid value1 type", a: "bad", b: t2, operator: Eq, wantErr: true},
		{name: "invalid value2 type", a: t1, b: "bad", operator: Eq, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compareTimestamp(tt.a, tt.b, tt.operator)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestOperatorString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want string
		op   Operator
	}{
		{want: "=", op: Eq},
		{want: "!=", op: Ne},
		{want: ">", op: Gt},
		{want: "<", op: Lt},
		{want: ">=", op: Gte},
		{want: "<=", op: Lte},
		{want: "IN", op: In},
		{want: "NOT IN", op: NotIn},
		{want: "LIKE", op: Like},
		{want: "NOT LIKE", op: NotLike},
		{want: "BETWEEN", op: Between},
		{want: "NOT BETWEEN", op: NotBetween},
		{want: "Unknown", op: Operator(999)},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.op.String())
		})
	}
}

func TestConditionOperands(t *testing.T) {
	t.Parallel()

	c := Condition{
		Operand1: Operand{Type: OperandField, Value: Field{Name: "age"}},
		Operator: Gt,
		Operand2: Operand{Type: OperandInteger, Value: int64(18)},
	}

	ops := c.Operands()
	require.Len(t, ops, 2)
	assert.Equal(t, c.Operand1, ops[0])
	assert.Equal(t, c.Operand2, ops[1])
}

func TestOneOrMoreLastCondition(t *testing.T) {
	t.Parallel()

	t.Run("empty returns false", func(t *testing.T) {
		var o OneOrMore
		_, ok := o.LastCondition()
		assert.False(t, ok)
	})

	t.Run("empty inner group returns false", func(t *testing.T) {
		o := OneOrMore{Conditions{}}
		_, ok := o.LastCondition()
		assert.False(t, ok)
	})

	t.Run("returns last condition", func(t *testing.T) {
		c1 := FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(1))
		c2 := FieldIsEqual(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte("alice")))
		o := OneOrMore{Conditions{c1, c2}}
		got, ok := o.LastCondition()
		require.True(t, ok)
		assert.Equal(t, c2, got)
	})
}

func TestOneOrMoreAppend(t *testing.T) {
	t.Parallel()

	t.Run("append to empty creates group", func(t *testing.T) {
		var o OneOrMore
		c := FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(1))
		o = o.Append(c)
		require.Len(t, o, 1)
		require.Len(t, o[0], 1)
		assert.Equal(t, c, o[0][0])
	})

	t.Run("append to existing group", func(t *testing.T) {
		c1 := FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(1))
		c2 := FieldIsEqual(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte("alice")))
		o := OneOrMore{Conditions{c1}}
		o = o.Append(c2)
		require.Len(t, o, 1)
		require.Len(t, o[0], 2)
		assert.Equal(t, c2, o[0][1])
	})
}

func TestOneOrMoreUpdateLast(t *testing.T) {
	t.Parallel()

	c1 := FieldIsEqual(Field{Name: "id"}, OperandInteger, int64(1))
	c2 := FieldIsEqual(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte("alice")))
	o := OneOrMore{Conditions{c1}}
	o.UpdateLast(c2)
	got, ok := o.LastCondition()
	require.True(t, ok)
	assert.Equal(t, c2, got)
}

func TestFieldIsLike(t *testing.T) {
	t.Parallel()

	c := FieldIsLike(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte("ali%")))
	assert.Equal(t, Like, c.Operator)
	assert.Equal(t, OperandField, c.Operand1.Type)
	assert.Equal(t, OperandQuotedString, c.Operand2.Type)
}

func TestFieldIsNotLike(t *testing.T) {
	t.Parallel()

	c := FieldIsNotLike(Field{Name: "name"}, OperandQuotedString, NewTextPointer([]byte("ali%")))
	assert.Equal(t, NotLike, c.Operator)
	assert.Equal(t, OperandField, c.Operand1.Type)
	assert.Equal(t, OperandQuotedString, c.Operand2.Type)
}

func TestFieldIsBetween(t *testing.T) {
	t.Parallel()

	c := FieldIsBetween(Field{Name: "age"}, int64(18), int64(65))
	assert.Equal(t, Between, c.Operator)
	assert.Equal(t, OperandField, c.Operand1.Type)
	assert.Equal(t, OperandList, c.Operand2.Type)
	vals, ok := c.Operand2.Value.([]any)
	require.True(t, ok)
	require.Len(t, vals, 2)
	assert.Equal(t, int64(18), vals[0])
	assert.Equal(t, int64(65), vals[1])
}

func TestFieldIsNotBetween(t *testing.T) {
	t.Parallel()

	c := FieldIsNotBetween(Field{Name: "age"}, int64(18), int64(65))
	assert.Equal(t, NotBetween, c.Operator)
	assert.Equal(t, OperandField, c.Operand1.Type)
	assert.Equal(t, OperandList, c.Operand2.Type)
	vals, ok := c.Operand2.Value.([]any)
	require.True(t, ok)
	require.Len(t, vals, 2)
	assert.Equal(t, int64(18), vals[0])
	assert.Equal(t, int64(65), vals[1])
}

func TestToInt64ForInt4(t *testing.T) {
	t.Parallel()

	t.Run("int64 value passes through", func(t *testing.T) {
		got, err := toInt64ForInt4(int64(42))
		require.NoError(t, err)
		assert.Equal(t, int64(42), got)
	})

	t.Run("int32 value is widened to int64", func(t *testing.T) {
		got, err := toInt64ForInt4(int32(-7))
		require.NoError(t, err)
		assert.Equal(t, int64(-7), got)
	})

	t.Run("wrong type returns error", func(t *testing.T) {
		_, err := toInt64ForInt4("not a number")
		assert.Error(t, err)
	})
}

func TestCompareInt4_TypeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		v1      any
		v2      any
		name    string
		op      Operator
		wantErr bool
	}{
		{name: "out of range v1", v1: int64(1 << 32), v2: int64(1), op: Eq, wantErr: true},
		{name: "out of range v2", v1: int64(1), v2: int64(1 << 32), op: Eq, wantErr: true},
		{name: "bad type v1", v1: "x", v2: int64(1), op: Eq, wantErr: true},
		{name: "bad type v2", v1: int64(1), v2: "x", op: Eq, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := compareInt4(tt.v1, tt.v2, tt.op)
			assert.Error(t, err)
		})
	}
}
