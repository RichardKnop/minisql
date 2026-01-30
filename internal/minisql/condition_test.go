package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsValidCondition(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		condition Condition
		isValid   bool
	}{
		{
			"valid condition with integer operand",
			Condition{
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
			true,
		},
		{
			"invalid condition with missing operand",
			Condition{
				Operand1: Operand{
					Type:  OperandField,
					Value: Field{Name: "a"},
				},
				Operator: Eq,
			},
			false,
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
		a        bool
		b        bool
		operator Operator
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
