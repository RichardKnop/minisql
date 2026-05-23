package minisql

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// ArithOp identifies the arithmetic operator in a binary Expr node.
type ArithOp int

// ArithOp constants.
const (
	ArithAdd       ArithOp = iota + 1 // +
	ArithSub                          // -
	ArithMul                          // *
	ArithDiv                          // /
	JSONArrow                         // -> (returns JSON fragment)
	JSONArrowArrow                    // ->> (returns SQL scalar)
)

func (op ArithOp) String() string {
	switch op {
	case ArithAdd:
		return "+"
	case ArithSub:
		return "-"
	case ArithMul:
		return "*"
	case ArithDiv:
		return "/"
	case JSONArrow:
		return "->"
	case JSONArrowArrow:
		return "->>"
	default:
		return "?"
	}
}

// CaseWhen holds one WHEN/THEN branch of a CASE expression.
// Exactly one of Cond (searched CASE) or When (simple CASE) is set.
type CaseWhen struct {
	Cond *ConditionNode // searched CASE: WHEN <condition>
	When *Expr          // simple CASE: WHEN <value> (compared to parent CaseInput)
	Then *Expr          // THEN <result>
}

// Expr is an arithmetic expression tree node.
// Exactly one interpretation is active (checked in priority order):
//   - WindowFunc != nil:        a window function call (ROW_NUMBER, SUM OVER, etc.)
//   - CaseClauses != nil:      a CASE WHEN expression
//   - FuncName != "":          a built-in function call (Args holds the arguments)
//   - CastExpr != nil:         a CAST(expr AS type) expression
//   - IsNull:                  an explicit NULL literal
//   - Column != "":            a column reference (read value from the row)
//   - Literal != nil:          a scalar literal (int64, float64, bool, TextPointer)
//   - Left != nil && Op != 0:  a binary arithmetic operation
type Expr struct {
	Literal        any
	Right          *Expr
	CaseElse       *Expr
	CaseInput      *Expr
	Left           *Expr
	CastExpr       *Expr
	WindowFunc     *WindowFunc
	FuncName       string
	Column         string
	Args           []*Expr
	CaseClauses    []CaseWhen
	CastTargetType ColumnKind
	Op             ArithOp
	IsNull         bool
}

// String returns a human-readable representation suitable for use as a default column name.
func (e *Expr) String() string {
	return e.str(false)
}

func (e *Expr) str(nested bool) string {
	if e == nil {
		return ""
	}
	if e.WindowFunc != nil {
		return windowFuncString(e.WindowFunc)
	}
	if e.CaseClauses != nil {
		var b strings.Builder
		b.WriteString("CASE")
		if e.CaseInput != nil {
			b.WriteString(" ")
			b.WriteString(e.CaseInput.str(false))
		}
		for _, cl := range e.CaseClauses {
			b.WriteString(" WHEN ")
			if cl.Cond != nil {
				b.WriteString(cl.Cond.String())
			} else {
				b.WriteString(cl.When.str(false))
			}
			b.WriteString(" THEN ")
			b.WriteString(cl.Then.str(false))
		}
		if e.CaseElse != nil {
			b.WriteString(" ELSE ")
			b.WriteString(e.CaseElse.str(false))
		}
		b.WriteString(" END")
		return b.String()
	}
	if e.FuncName != "" {
		argStrs := make([]string, len(e.Args))
		for i, arg := range e.Args {
			argStrs[i] = arg.str(false)
		}
		return e.FuncName + "(" + strings.Join(argStrs, ", ") + ")"
	}
	if e.CastExpr != nil {
		return "CAST(" + e.CastExpr.str(false) + " AS " + e.CastTargetType.String() + ")"
	}
	if e.IsNull {
		return "NULL"
	}
	if e.Column != "" {
		return e.Column
	}
	if e.Literal != nil {
		switch v := e.Literal.(type) {
		case TextPointer:
			return string(v.Data)
		case Interval:
			return v.String()
		default:
			return fmt.Sprintf("%v", e.Literal)
		}
	}
	inner := e.Left.str(true) + " " + e.Op.String() + " " + e.Right.str(true)
	if nested {
		return "(" + inner + ")"
	}
	return inner
}

// Columns returns all column names referenced by this expression.
func (e *Expr) Columns() []string {
	if e == nil {
		return nil
	}
	if e.CaseClauses != nil {
		var cols []string
		cols = append(cols, e.CaseInput.Columns()...)
		for _, cl := range e.CaseClauses {
			if cl.Cond != nil {
				cols = append(cols, cl.Cond.Columns()...)
			}
			if cl.When != nil {
				cols = append(cols, cl.When.Columns()...)
			}
			cols = append(cols, cl.Then.Columns()...)
		}
		cols = append(cols, e.CaseElse.Columns()...)
		return cols
	}
	if e.FuncName != "" {
		var cols []string
		for _, arg := range e.Args {
			cols = append(cols, arg.Columns()...)
		}
		return cols
	}
	if e.CastExpr != nil {
		return e.CastExpr.Columns()
	}
	if e.IsNull {
		return nil
	}
	if e.Column != "" {
		return []string{e.Column}
	}
	if e.Literal != nil {
		return nil
	}
	left := e.Left.Columns()
	right := e.Right.Columns()
	return append(left, right...)
}

// ColumnRefs returns all column names referenced in the expression tree.
func (e *Expr) ColumnRefs() []string {
	if e == nil {
		return nil
	}
	if e.Column != "" {
		return []string{e.Column}
	}
	var cols []string
	cols = append(cols, e.Left.ColumnRefs()...)
	cols = append(cols, e.Right.ColumnRefs()...)
	for _, arg := range e.Args {
		cols = append(cols, arg.ColumnRefs()...)
	}
	return cols
}

// windowFuncString returns a human-readable name for a window function expression.
func windowFuncString(wf *WindowFunc) string {
	if wf == nil {
		return ""
	}
	switch wf.Kind {
	case WindowRowNumber:
		return "ROW_NUMBER() OVER (...)"
	case WindowRank:
		return "RANK() OVER (...)"
	case WindowDenseRank:
		return "DENSE_RANK() OVER (...)"
	case WindowNtile:
		return "NTILE(...) OVER (...)"
	case WindowLag:
		return "LAG(...) OVER (...)"
	case WindowLead:
		return "LEAD(...) OVER (...)"
	case WindowFirstValue:
		return "FIRST_VALUE(...) OVER (...)"
	case WindowLastValue:
		return "LAST_VALUE(...) OVER (...)"
	case WindowNthValue:
		return "NTH_VALUE(...) OVER (...)"
	case WindowSum:
		return "SUM(...) OVER (...)"
	case WindowAvg:
		return "AVG(...) OVER (...)"
	case WindowCount:
		return "COUNT(*) OVER (...)"
	case WindowMin:
		return "MIN(...) OVER (...)"
	case WindowMax:
		return "MAX(...) OVER (...)"
	}
	return "WINDOW(...)"
}

// Eval evaluates the expression against a row, returning a numeric result.
// NULL propagates: any NULL operand produces a nil result.
// Returns int64 when both operands are int64 (except division, which always returns float64).
// Returns float64 when either operand is float64.
func (e *Expr) Eval(row Row) (any, error) {
	if e == nil {
		return nil, nil
	}

	// Window function expressions cannot be evaluated row-by-row; they require
	// partition context.  selectWithWindowFuncs handles them before projection.
	if e.WindowFunc != nil {
		return nil, fmt.Errorf("window function %s cannot be evaluated outside window context", windowFuncString(e.WindowFunc))
	}

	// CASE expression
	if e.CaseClauses != nil {
		return e.evalCase(row)
	}

	// Function call
	if e.FuncName != "" {
		return e.evalFunc(row)
	}

	// CAST expression
	if e.CastExpr != nil {
		return e.evalCast(row)
	}

	// Explicit NULL literal
	if e.IsNull {
		return nil, nil
	}

	// Column reference
	if e.Column != "" {
		// Support alias.col lookup (used in JOIN projections)
		name := e.Column
		v, ok := row.GetValue(name)
		if !ok {
			// Try stripping alias prefix for plain column ref
			if dot := strings.LastIndex(name, "."); dot >= 0 {
				v, ok = row.GetValue(name[dot+1:])
			}
			if !ok {
				return nil, fmt.Errorf("column %q not found in row", e.Column)
			}
		}
		if !v.Valid {
			return nil, nil // NULL propagates
		}
		return v.Value, nil
	}

	// Numeric literal
	if e.Literal != nil {
		return e.Literal, nil
	}

	// Binary expression
	leftVal, err := e.Left.Eval(row)
	if err != nil {
		return nil, err
	}
	rightVal, err := e.Right.Eval(row)
	if err != nil {
		return nil, err
	}

	// NULL propagation
	if leftVal == nil || rightVal == nil {
		return nil, nil
	}

	// Timestamp ± Interval → Timestamp (stored as TimestampMicros)
	if lt, lok := leftVal.(TimestampMicros); lok {
		if ri, rok := rightVal.(Interval); rok {
			if e.Op != ArithAdd && e.Op != ArithSub {
				return nil, fmt.Errorf("operator %s is not defined for timestamp and interval", e.Op)
			}
			sign := int32(1)
			if e.Op == ArithSub {
				sign = -1
			}
			return TimestampMicros(FromMicroseconds(int64(lt)).AddInterval(ri, sign).TotalMicroseconds()), nil
		}
	}
	// Interval + Timestamp → Timestamp (addition is commutative)
	if li, lok := leftVal.(Interval); lok {
		if rt, rok := rightVal.(TimestampMicros); rok {
			if e.Op != ArithAdd {
				return nil, fmt.Errorf("operator %s is not defined for interval and timestamp", e.Op)
			}
			return TimestampMicros(FromMicroseconds(int64(rt)).AddInterval(li, 1).TotalMicroseconds()), nil
		}
	}
	// Interval ± Interval → Interval
	if li, lok := leftVal.(Interval); lok {
		if ri, rok := rightVal.(Interval); rok {
			if e.Op != ArithAdd && e.Op != ArithSub {
				return nil, fmt.Errorf("operator %s is not defined for interval and interval", e.Op)
			}
			sign := int32(1)
			if e.Op == ArithSub {
				sign = -1
			}
			return Interval{
				Months: li.Months + sign*ri.Months,
				Micros: li.Micros + int64(sign)*ri.Micros,
			}, nil
		}
	}
	// Timestamp - Timestamp → Interval (fixed-duration difference only)
	if lt, lok := leftVal.(TimestampMicros); lok {
		if rt, rok := rightVal.(TimestampMicros); rok {
			if e.Op != ArithSub {
				return nil, fmt.Errorf("operator %s is not defined for two timestamps", e.Op)
			}
			return Interval{Micros: int64(lt) - int64(rt)}, nil
		}
	}

	// JSON operators must be handled before numeric coercion.
	if e.Op == JSONArrow || e.Op == JSONArrowArrow {
		return e.evalJSONOp(leftVal, rightVal)
	}

	lf, err := toFloat64(leftVal)
	if err != nil {
		return nil, fmt.Errorf("left operand of %s: %w", e.Op, err)
	}
	rf, err := toFloat64(rightVal)
	if err != nil {
		return nil, fmt.Errorf("right operand of %s: %w", e.Op, err)
	}

	// Try integer arithmetic when both sides are integral (avoids float64 rounding).
	li, leftIsInt := toInt64(leftVal)
	ri, rightIsInt := toInt64(rightVal)

	switch e.Op {
	case ArithAdd:
		if leftIsInt && rightIsInt {
			return li + ri, nil
		}
		return lf + rf, nil
	case ArithSub:
		if leftIsInt && rightIsInt {
			return li - ri, nil
		}
		return lf - rf, nil
	case ArithMul:
		if leftIsInt && rightIsInt {
			return li * ri, nil
		}
		return lf * rf, nil
	case ArithDiv:
		if rf == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return lf / rf, nil
	default:
		return nil, fmt.Errorf("unknown arithmetic operator %d", e.Op)
	}
}

// evalCast evaluates CAST(expr AS type) against the given row.
// NULL propagates: a NULL inner expression produces nil.
// Coercion rules follow SQLite semantics: truncation for float→int, decimal strings for
// numeric→text, leading-digit parsing for text→int/float.
func (e *Expr) evalCast(row Row) (any, error) {
	val, err := e.CastExpr.Eval(row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil // NULL propagates
	}

	switch e.CastTargetType {
	case Boolean:
		return castToBool(val)
	case Int4, Int8:
		n, err := castToInt64(val)
		if err != nil {
			return nil, err
		}
		if e.CastTargetType == Int4 {
			if n > math.MaxInt32 || n < math.MinInt32 {
				return nil, fmt.Errorf("CAST: value %d overflows INT4", n)
			}
			return int32(n), nil
		}
		return n, nil
	case Real:
		f, err := castToFloat64(val)
		if err != nil {
			return nil, err
		}
		return float32(f), nil
	case Double:
		return castToFloat64(val)
	case Text, Varchar:
		return castToTextPointer(val)
	case Timestamp:
		return castToTimestamp(val)
	case JSON:
		s, ok := toStringVal(val)
		if !ok {
			return nil, fmt.Errorf("CAST: cannot convert %T to JSON", val)
		}
		normalised, err := normaliseJSON(s)
		if err != nil {
			return nil, fmt.Errorf("CAST: %w", err)
		}
		return NewTextPointer([]byte(normalised)), nil
	case UUID:
		switch v := val.(type) {
		case UUIDValue:
			return v, nil
		case TextPointer:
			uv, err := ParseUUID(v.String())
			if err != nil {
				return nil, fmt.Errorf("CAST: %w", err)
			}
			return uv, nil
		default:
			s, ok := toStringVal(val)
			if !ok {
				return nil, fmt.Errorf("CAST: cannot convert %T to UUID", val)
			}
			uv, err := ParseUUID(s)
			if err != nil {
				return nil, fmt.Errorf("CAST: %w", err)
			}
			return uv, nil
		}
	default:
		return nil, fmt.Errorf("CAST: unsupported target type %v", e.CastTargetType)
	}
}

// evalJSONOp evaluates the -> and ->> binary JSON operators.
// Left must be a JSON text value; right must be a string key or integer index.
func (e *Expr) evalJSONOp(leftVal, rightVal any) (any, error) {
	docStr, ok := toStringVal(leftVal)
	if !ok {
		return nil, fmt.Errorf("operator %s: left operand must be a JSON string, got %T", e.Op, leftVal)
	}
	var parsed any
	if err := json.Unmarshal([]byte(docStr), &parsed); err != nil {
		return nil, fmt.Errorf("operator %s: invalid JSON: %w", e.Op, err)
	}

	var extracted any
	switch key := rightVal.(type) {
	case TextPointer:
		obj, ok := parsed.(map[string]any)
		if !ok {
			return nil, nil // not an object — NULL
		}
		v, exists := obj[key.String()]
		if !exists {
			return nil, nil
		}
		extracted = v
	case int64:
		arr, ok := parsed.([]any)
		if !ok {
			return nil, nil // not an array — NULL
		}
		if key < 0 || key >= int64(len(arr)) {
			return nil, nil
		}
		extracted = arr[key]
	default:
		return nil, fmt.Errorf("operator %s: right operand must be a string key or integer index, got %T", e.Op, rightVal)
	}

	if extracted == nil {
		return nil, nil
	}
	if e.Op == JSONArrow {
		b, _ := json.Marshal(extracted)
		return NewTextPointer(b), nil
	}
	return jsonToScalar(extracted), nil
}

func castToBool(v any) (bool, error) {
	switch n := v.(type) {
	case bool:
		return n, nil
	case int64:
		return n != 0, nil
	case int32:
		return n != 0, nil
	case float64:
		return n != 0, nil
	case float32:
		return n != 0, nil
	case TextPointer:
		s := string(n.Data)
		// SQLite: non-empty string that starts with a non-zero digit → true
		i, ok := toInt64(s)
		if ok {
			return i != 0, nil
		}
		f, ok2 := toFloat64FromString(s)
		if ok2 {
			return f != 0, nil
		}
		return false, nil
	default:
		return false, fmt.Errorf("CAST: cannot convert %T to BOOLEAN", v)
	}
}

func castToInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int32:
		return int64(n), nil
	case float64:
		return int64(n), nil // truncate toward zero (SQLite)
	case float32:
		return int64(n), nil
	case bool:
		if n {
			return 1, nil
		}
		return 0, nil
	case TextPointer:
		s := string(n.Data)
		if i, ok := toInt64FromString(s); ok {
			return i, nil
		}
		// SQLite: leading float digits are accepted (e.g. "3.9" → 3)
		if f, ok := toFloat64FromString(s); ok {
			return int64(f), nil
		}
		return 0, nil // no leading digits → 0 (SQLite semantics)
	default:
		return 0, fmt.Errorf("CAST: cannot convert %T to integer", v)
	}
}

func castToFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case int32:
		return float64(n), nil
	case bool:
		if n {
			return 1.0, nil
		}
		return 0.0, nil
	case TextPointer:
		s := string(n.Data)
		f, ok := toFloat64FromString(s)
		if !ok {
			return 0.0, nil // no leading digits → 0.0 (SQLite semantics)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("CAST: cannot convert %T to float", v)
	}
}

func castToTextPointer(v any) (TextPointer, error) {
	switch n := v.(type) {
	case TextPointer:
		return n, nil
	case int64:
		return NewTextPointer([]byte(fmt.Sprintf("%d", n))), nil
	case int32:
		return NewTextPointer([]byte(fmt.Sprintf("%d", n))), nil
	case float64:
		return NewTextPointer([]byte(strconv.FormatFloat(n, 'f', -1, 64))), nil
	case float32:
		return NewTextPointer([]byte(strconv.FormatFloat(float64(n), 'f', -1, 32))), nil
	case bool:
		if n {
			return NewTextPointer([]byte("1")), nil
		}
		return NewTextPointer([]byte("0")), nil
	case Time:
		return NewTextPointer([]byte(n.GoTime().UTC().Format("2006-01-02 15:04:05"))), nil
	case UUIDValue:
		return NewTextPointer([]byte(n.String())), nil
	default:
		return TextPointer{}, fmt.Errorf("CAST: cannot convert %T to text", v)
	}
}

func castToTimestamp(v any) (TimestampMicros, error) {
	switch n := v.(type) {
	case TimestampMicros:
		return n, nil
	case TextPointer:
		t, err := ParseTimestamp(string(n.Data))
		if err != nil {
			return 0, fmt.Errorf("CAST: %w", err)
		}
		return TimestampMicros(t.TotalMicroseconds()), nil
	default:
		return 0, fmt.Errorf("CAST: cannot convert %T to TIMESTAMP", v)
	}
}

// toInt64FromString parses the leading integer from a string (SQLite semantics).
// Returns (0, false) when there are no leading digits.
func toInt64FromString(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	end := 0
	if end < len(s) && (s[end] == '+' || s[end] == '-') {
		end += 1
	}
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end += 1
	}
	if end == 0 || (end == 1 && (s[0] == '+' || s[0] == '-')) {
		return 0, false
	}
	n, err := strconv.ParseInt(s[:end], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// toFloat64FromString parses the leading float from a string (SQLite semantics).
// Returns (0, false) when there are no leading digits.
func toFloat64FromString(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	// Find the end of the leading numeric portion.
	end := 0
	if end < len(s) && (s[end] == '+' || s[end] == '-') {
		end += 1
	}
	for end < len(s) && (s[end] >= '0' && s[end] <= '9' || s[end] == '.') {
		end += 1
	}
	// Optional exponent
	if end < len(s) && (s[end] == 'e' || s[end] == 'E') {
		end += 1
		if end < len(s) && (s[end] == '+' || s[end] == '-') {
			end += 1
		}
		for end < len(s) && s[end] >= '0' && s[end] <= '9' {
			end += 1
		}
	}
	if end == 0 {
		return 0, false
	}
	f, err := strconv.ParseFloat(s[:end], 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// evalCase evaluates a CASE WHEN expression against the given row.
func (e *Expr) evalCase(row Row) (any, error) {
	// For simple CASE, evaluate the input expression once.
	var inputVal any
	if e.CaseInput != nil {
		var err error
		inputVal, err = e.CaseInput.Eval(row)
		if err != nil {
			return nil, err
		}
	}

	for _, clause := range e.CaseClauses {
		var matched bool
		if e.CaseInput != nil {
			// Simple CASE: compare input to WHEN value.
			whenVal, err := clause.When.Eval(row)
			if err != nil {
				return nil, err
			}
			matched = equalAny(inputVal, whenVal)
		} else {
			// Searched CASE: evaluate WHEN condition against the row.
			var err error
			matched, err = row.CheckOneOrMore(clause.Cond.ToDNF())
			if err != nil {
				return nil, err
			}
		}
		if matched {
			return clause.Then.Eval(row)
		}
	}

	// No WHEN matched — return ELSE value (or NULL if no ELSE).
	if e.CaseElse != nil {
		return e.CaseElse.Eval(row)
	}
	return nil, nil
}

// evalFunc evaluates a built-in function call against the given row.
func (e *Expr) evalFunc(row Row) (any, error) {
	switch e.FuncName {
	case "COALESCE":
		if len(e.Args) == 0 {
			return nil, fmt.Errorf("COALESCE requires at least 1 argument")
		}
		for _, arg := range e.Args {
			val, err := arg.Eval(row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				return val, nil
			}
		}
		return nil, nil // all arguments were NULL
	case "NULLIF":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("NULLIF requires exactly 2 arguments, got %d", len(e.Args))
		}
		a, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if a == nil {
			// For example: NULLIF(NULL, x) = NULL
			return nil, nil
		}
		b, err := e.Args[1].Eval(row)
		if err != nil {
			return nil, err
		}
		if equalAny(a, b) {
			return nil, nil // equal → return NULL
		}
		return a, nil
	// ── String functions ────────────────────────────────────────────────────

	case "UPPER", "LOWER":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("%s requires exactly 1 argument", e.FuncName)
		}
		v, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, nil
		}
		s, ok := toStringVal(v)
		if !ok {
			return nil, fmt.Errorf("%s: argument must be a string, got %T", e.FuncName, v)
		}
		if e.FuncName == "UPPER" {
			return NewTextPointer([]byte(strings.ToUpper(s))), nil
		}
		return NewTextPointer([]byte(strings.ToLower(s))), nil

	case "TRIM", "LTRIM", "RTRIM":
		if len(e.Args) < 1 || len(e.Args) > 2 {
			return nil, fmt.Errorf("%s requires 1 or 2 arguments", e.FuncName)
		}
		v, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, nil
		}
		s, ok := toStringVal(v)
		if !ok {
			return nil, fmt.Errorf("%s: argument 1 must be a string, got %T", e.FuncName, v)
		}
		cutset := " \t\n\r"
		if len(e.Args) == 2 {
			cv, err := e.Args[1].Eval(row)
			if err != nil {
				return nil, err
			}
			if cv == nil {
				return nil, nil
			}
			cs, ok := toStringVal(cv)
			if !ok {
				return nil, fmt.Errorf("%s: argument 2 must be a string, got %T", e.FuncName, cv)
			}
			cutset = cs
		}
		var result string
		switch e.FuncName {
		case "TRIM":
			result = strings.Trim(s, cutset)
		case "LTRIM":
			result = strings.TrimLeft(s, cutset)
		default: // RTRIM
			result = strings.TrimRight(s, cutset)
		}
		return NewTextPointer([]byte(result)), nil

	case "LENGTH":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("LENGTH requires exactly 1 argument")
		}
		v, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, nil
		}
		s, ok := toStringVal(v)
		if !ok {
			return nil, fmt.Errorf("LENGTH: argument must be a string, got %T", v)
		}
		return int64(len(s)), nil

	case "SUBSTR":
		if len(e.Args) < 2 || len(e.Args) > 3 {
			return nil, fmt.Errorf("SUBSTR requires 2 or 3 arguments")
		}
		sv, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if sv == nil {
			return nil, nil
		}
		s, ok := toStringVal(sv)
		if !ok {
			return nil, fmt.Errorf("SUBSTR: argument 1 must be a string, got %T", sv)
		}
		startVal, err := e.Args[1].Eval(row)
		if err != nil {
			return nil, err
		}
		if startVal == nil {
			return nil, nil
		}
		start, ok := toInt64(startVal)
		if !ok {
			return nil, fmt.Errorf("SUBSTR: start must be an integer, got %T", startVal)
		}
		// SQL uses 1-based indexing; clamp to valid range.
		start -= 1 // convert to 0-based
		if start < 0 {
			start = 0
		}
		b := []byte(s)
		if start >= int64(len(b)) {
			return NewTextPointer([]byte{}), nil
		}
		if len(e.Args) == 3 {
			lenVal, err := e.Args[2].Eval(row)
			if err != nil {
				return nil, err
			}
			if lenVal == nil {
				return nil, nil
			}
			length, ok := toInt64(lenVal)
			if !ok {
				return nil, fmt.Errorf("SUBSTR: length must be an integer, got %T", lenVal)
			}
			if length < 0 {
				length = 0
			}
			end := start + length
			if end > int64(len(b)) {
				end = int64(len(b))
			}
			return NewTextPointer(b[start:end]), nil
		}
		return NewTextPointer(b[start:]), nil

	case "REPLACE":
		if len(e.Args) != 3 {
			return nil, fmt.Errorf("REPLACE requires exactly 3 arguments")
		}
		sv, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if sv == nil {
			return nil, nil
		}
		s, ok := toStringVal(sv)
		if !ok {
			return nil, fmt.Errorf("REPLACE: argument 1 must be a string, got %T", sv)
		}
		fromVal, err := e.Args[1].Eval(row)
		if err != nil {
			return nil, err
		}
		if fromVal == nil {
			return nil, nil
		}
		from, ok := toStringVal(fromVal)
		if !ok {
			return nil, fmt.Errorf("REPLACE: argument 2 must be a string, got %T", fromVal)
		}
		toVal, err := e.Args[2].Eval(row)
		if err != nil {
			return nil, err
		}
		if toVal == nil {
			return nil, nil
		}
		to, ok := toStringVal(toVal)
		if !ok {
			return nil, fmt.Errorf("REPLACE: argument 3 must be a string, got %T", toVal)
		}
		return NewTextPointer([]byte(strings.ReplaceAll(s, from, to))), nil

	case "CONCAT":
		if len(e.Args) == 0 {
			return nil, fmt.Errorf("CONCAT requires at least 1 argument")
		}
		var buf strings.Builder
		for i, arg := range e.Args {
			v, err := arg.Eval(row)
			if err != nil {
				return nil, err
			}
			if v == nil {
				continue // skip NULLs (PostgreSQL semantics)
			}
			s, ok := toStringVal(v)
			if !ok {
				return nil, fmt.Errorf("CONCAT: argument %d must be a string, got %T", i+1, v)
			}
			buf.WriteString(s)
		}
		return NewTextPointer([]byte(buf.String())), nil

	// ── Numeric functions ────────────────────────────────────────────────────

	case "ABS":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("ABS requires exactly 1 argument")
		}
		v, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, nil
		}
		switch n := v.(type) {
		case int32:
			if n < 0 {
				return -n, nil
			}
			return n, nil
		case int64:
			if n < 0 {
				return -n, nil
			}
			return n, nil
		case float32:
			if n < 0 {
				return -n, nil
			}
			return n, nil
		case float64:
			if n < 0 {
				return -n, nil
			}
			return n, nil
		default:
			return nil, fmt.Errorf("ABS: argument must be numeric, got %T", v)
		}

	case "FLOOR", "CEIL":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("%s requires exactly 1 argument", e.FuncName)
		}
		v, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, nil
		}
		// Integer inputs are already whole numbers — return as-is.
		if n, ok := toInt64(v); ok {
			return n, nil
		}
		f, ferr := toFloat64(v)
		if ferr != nil {
			return nil, fmt.Errorf("%s: argument must be numeric, got %T", e.FuncName, v)
		}
		if e.FuncName == "FLOOR" {
			return math.Floor(f), nil
		}
		return math.Ceil(f), nil

	case "ROUND":
		if len(e.Args) < 1 || len(e.Args) > 2 {
			return nil, fmt.Errorf("ROUND requires 1 or 2 arguments")
		}
		v, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, nil
		}
		d := int64(0)
		if len(e.Args) == 2 {
			dv, err := e.Args[1].Eval(row)
			if err != nil {
				return nil, err
			}
			if dv == nil {
				return nil, nil
			}
			var ok bool
			d, ok = toInt64(dv)
			if !ok {
				return nil, fmt.Errorf("ROUND: decimal places must be an integer, got %T", dv)
			}
		}
		// Integer inputs: rounding has no effect regardless of d.
		if n, ok := toInt64(v); ok {
			return n, nil
		}
		f, ferr := toFloat64(v)
		if ferr != nil {
			return nil, fmt.Errorf("ROUND: argument must be numeric, got %T", v)
		}
		if d == 0 {
			return math.Round(f), nil
		}
		factor := math.Pow(10, float64(d))
		return math.Round(f*factor) / factor, nil

	case "MOD":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("MOD requires exactly 2 arguments")
		}
		av, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if av == nil {
			return nil, nil
		}
		bv, err := e.Args[1].Eval(row)
		if err != nil {
			return nil, err
		}
		if bv == nil {
			return nil, nil
		}
		// Integer fast-path.
		ai, aIsInt := toInt64(av)
		bi, bIsInt := toInt64(bv)
		if aIsInt && bIsInt {
			if bi == 0 {
				return nil, fmt.Errorf("MOD: division by zero")
			}
			return ai % bi, nil
		}
		af, aerr := toFloat64(av)
		bf, berr := toFloat64(bv)
		if aerr != nil {
			return nil, fmt.Errorf("MOD: argument 1 must be numeric, got %T", av)
		}
		if berr != nil {
			return nil, fmt.Errorf("MOD: argument 2 must be numeric, got %T", bv)
		}
		if bf == 0 {
			return nil, fmt.Errorf("MOD: division by zero")
		}
		return math.Mod(af, bf), nil

	// ── Date/time functions ──────────────────────────────────────────────────

	case "NOW":
		if len(e.Args) != 0 {
			return nil, fmt.Errorf("NOW takes no arguments")
		}
		now := time.Now().UTC()
		return TimestampMicros(Time{
			Year:         int32(now.Year()),
			Month:        int8(now.Month()),
			Day:          int8(now.Day()),
			Hour:         int8(now.Hour()),
			Minutes:      int8(now.Minute()),
			Seconds:      int8(now.Second()),
			Microseconds: int32(now.Nanosecond() / 1000),
		}.TotalMicroseconds()), nil

	case "DATE_TRUNC":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("DATE_TRUNC requires exactly 2 arguments")
		}
		unitVal, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if unitVal == nil {
			return nil, nil
		}
		unit, ok := toStringVal(unitVal)
		if !ok {
			return nil, fmt.Errorf("DATE_TRUNC: first argument must be a string, got %T", unitVal)
		}
		tsVal, err := e.Args[1].Eval(row)
		if err != nil {
			return nil, err
		}
		if tsVal == nil {
			return nil, nil
		}
		tsMicrosRaw, ok := tsVal.(TimestampMicros)
		if !ok {
			return nil, fmt.Errorf("DATE_TRUNC: second argument must be a timestamp, got %T", tsVal)
		}
		ts := FromMicroseconds(int64(tsMicrosRaw))
		switch strings.ToLower(unit) {
		case "year":
			return TimestampMicros(Time{Year: ts.Year, Month: 1, Day: 1}.TotalMicroseconds()), nil
		case "month":
			return TimestampMicros(Time{Year: ts.Year, Month: ts.Month, Day: 1}.TotalMicroseconds()), nil
		case "day":
			return TimestampMicros(Time{Year: ts.Year, Month: ts.Month, Day: ts.Day}.TotalMicroseconds()), nil
		case "hour":
			return TimestampMicros(Time{Year: ts.Year, Month: ts.Month, Day: ts.Day, Hour: ts.Hour}.TotalMicroseconds()), nil
		case "minute":
			return TimestampMicros(Time{Year: ts.Year, Month: ts.Month, Day: ts.Day, Hour: ts.Hour, Minutes: ts.Minutes}.TotalMicroseconds()), nil
		case "second":
			return TimestampMicros(Time{Year: ts.Year, Month: ts.Month, Day: ts.Day, Hour: ts.Hour, Minutes: ts.Minutes, Seconds: ts.Seconds}.TotalMicroseconds()), nil
		default:
			return nil, fmt.Errorf("DATE_TRUNC: unknown unit %q (want year/month/day/hour/minute/second)", unit)
		}

	case "EXTRACT", "DATE_PART":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("%s requires exactly 2 arguments", e.FuncName)
		}
		fieldVal, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if fieldVal == nil {
			return nil, nil
		}
		field, ok := toStringVal(fieldVal)
		if !ok {
			return nil, fmt.Errorf("%s: first argument must be a string, got %T", e.FuncName, fieldVal)
		}
		tsVal, err := e.Args[1].Eval(row)
		if err != nil {
			return nil, err
		}
		if tsVal == nil {
			return nil, nil
		}
		tsMicrosRaw, ok := tsVal.(TimestampMicros)
		if !ok {
			return nil, fmt.Errorf("%s: second argument must be a timestamp, got %T", e.FuncName, tsVal)
		}
		ts := FromMicroseconds(int64(tsMicrosRaw))
		switch strings.ToLower(field) {
		case "year":
			return int64(ts.Year), nil
		case "month":
			return int64(ts.Month), nil
		case "day":
			return int64(ts.Day), nil
		case "hour":
			return int64(ts.Hour), nil
		case "minute":
			return int64(ts.Minutes), nil
		case "second":
			return int64(ts.Seconds), nil
		case "microsecond":
			return int64(ts.Microseconds), nil
		case "epoch":
			return int64(tsMicrosRaw) / microsecondsInSecond, nil
		default:
			return nil, fmt.Errorf("%s: unknown field %q (want year/month/day/hour/minute/second/microsecond/epoch)", e.FuncName, field)
		}

	case "TO_TIMESTAMP":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("TO_TIMESTAMP requires exactly 1 argument")
		}
		v, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, nil
		}
		s, ok := toStringVal(v)
		if !ok {
			return nil, fmt.Errorf("TO_TIMESTAMP: argument must be a string, got %T", v)
		}
		t, err := ParseTimestamp(s)
		if err != nil {
			return nil, fmt.Errorf("TO_TIMESTAMP: %w", err)
		}
		return TimestampMicros(t.TotalMicroseconds()), nil

	// ── Text search functions ──────────────────────────────────────────────────

	case "MATCH":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("MATCH requires exactly 2 arguments")
		}
		docVal, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		queryVal, err := e.Args[1].Eval(row)
		if err != nil {
			return nil, err
		}
		if docVal == nil || queryVal == nil {
			return false, nil
		}
		doc, ok := toStringVal(docVal)
		if !ok {
			return nil, fmt.Errorf("MATCH: first argument must be a string")
		}
		query, ok := toStringVal(queryVal)
		if !ok {
			return nil, fmt.Errorf("MATCH: second argument must be a string")
		}
		return textSearchMatch(doc, query), nil

	case "TS_RANK":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("TS_RANK requires exactly 2 arguments")
		}
		docVal, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		queryVal, err := e.Args[1].Eval(row)
		if err != nil {
			return nil, err
		}
		if docVal == nil || queryVal == nil {
			return float64(0), nil
		}
		doc, ok := toStringVal(docVal)
		if !ok {
			return nil, fmt.Errorf("TS_RANK: first argument must be a string")
		}
		query, ok := toStringVal(queryVal)
		if !ok {
			return nil, fmt.Errorf("TS_RANK: second argument must be a string")
		}
		return textSearchRank(doc, query), nil

	// ── JSON functions ──────────────────────────────────────────────────────────

	case "JSON_VALID":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("JSON_VALID requires exactly 1 argument")
		}
		v, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, nil
		}
		s, ok := toStringVal(v)
		if !ok {
			return int64(0), nil
		}
		if !json.Valid([]byte(s)) {
			return int64(0), nil
		}
		return int64(1), nil

	case "JSON_TYPE":
		if len(e.Args) == 0 || len(e.Args) > 2 {
			return nil, fmt.Errorf("JSON_TYPE requires 1 or 2 arguments")
		}
		docVal, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if docVal == nil {
			return nil, nil
		}
		docStr, ok := toStringVal(docVal)
		if !ok {
			return nil, fmt.Errorf("JSON_TYPE: first argument must be a string")
		}
		var parsed any
		if err := json.Unmarshal([]byte(docStr), &parsed); err != nil {
			return nil, fmt.Errorf("JSON_TYPE: invalid JSON: %w", err)
		}
		target := parsed
		if len(e.Args) == 2 {
			pathVal, err := e.Args[1].Eval(row)
			if err != nil {
				return nil, err
			}
			if pathVal == nil {
				return nil, nil
			}
			pathStr, ok := toStringVal(pathVal)
			if !ok {
				return nil, fmt.Errorf("JSON_TYPE: path argument must be a string")
			}
			val, found, err := evalJSONPath(parsed, pathStr)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, nil
			}
			target = val
		}
		return NewTextPointer([]byte(jsonTypeName(target))), nil

	case "JSON_ARRAY_LENGTH":
		if len(e.Args) == 0 || len(e.Args) > 2 {
			return nil, fmt.Errorf("JSON_ARRAY_LENGTH requires 1 or 2 arguments")
		}
		docVal, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if docVal == nil {
			return nil, nil
		}
		docStr, ok := toStringVal(docVal)
		if !ok {
			return nil, fmt.Errorf("JSON_ARRAY_LENGTH: first argument must be a string")
		}
		var parsed any
		if err := json.Unmarshal([]byte(docStr), &parsed); err != nil {
			return nil, fmt.Errorf("JSON_ARRAY_LENGTH: invalid JSON: %w", err)
		}
		target := parsed
		if len(e.Args) == 2 {
			pathVal, err := e.Args[1].Eval(row)
			if err != nil {
				return nil, err
			}
			if pathVal == nil {
				return nil, nil
			}
			pathStr, ok := toStringVal(pathVal)
			if !ok {
				return nil, fmt.Errorf("JSON_ARRAY_LENGTH: path argument must be a string")
			}
			val, found, err := evalJSONPath(parsed, pathStr)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, nil
			}
			target = val
		}
		arr, ok := target.([]any)
		if !ok {
			return nil, nil // not an array — NULL
		}
		return int64(len(arr)), nil

	case "JSON_EXTRACT":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("JSON_EXTRACT requires exactly 2 arguments")
		}
		docVal, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		if docVal == nil {
			return nil, nil
		}
		pathVal, err := e.Args[1].Eval(row)
		if err != nil {
			return nil, err
		}
		if pathVal == nil {
			return nil, nil
		}
		docStr, ok := toStringVal(docVal)
		if !ok {
			return nil, fmt.Errorf("JSON_EXTRACT: first argument must be a string")
		}
		pathStr, ok := toStringVal(pathVal)
		if !ok {
			return nil, fmt.Errorf("JSON_EXTRACT: second argument must be a string")
		}
		_, scalar, found, err := jsonExtract(docStr, pathStr)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, nil
		}
		return scalar, nil

	case "JSON_CONTAINS":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("JSON_CONTAINS requires exactly 2 arguments")
		}
		docVal, err := e.Args[0].Eval(row)
		if err != nil {
			return nil, err
		}
		queryVal, err := e.Args[1].Eval(row)
		if err != nil {
			return nil, err
		}
		if docVal == nil || queryVal == nil {
			return false, nil
		}
		docStr, ok := toStringVal(docVal)
		if !ok {
			return nil, fmt.Errorf("JSON_CONTAINS: first argument must be a string")
		}
		queryStr, ok := toStringVal(queryVal)
		if !ok {
			return nil, fmt.Errorf("JSON_CONTAINS: second argument must be a string")
		}
		return jsonContains(docStr, queryStr)

	default:
		return nil, fmt.Errorf("unknown function %q", e.FuncName)
	}
}

// toStringVal extracts the string content from a TextPointer or plain string value.
// Returns false if v is neither type.
func toStringVal(v any) (string, bool) {
	switch s := v.(type) {
	case TextPointer:
		return string(s.Data), true
	case string:
		return s, true
	}
	return "", false
}

// toInt64 returns an int64 if v is an integer type (int32 or int64), otherwise false.
func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	default:
		return 0, false
	}
}

func toFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case int64:
		return float64(n), nil
	case float64:
		return n, nil
	case int32:
		return float64(n), nil
	case float32:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("cannot use %T as a numeric operand", v)
	}
}
