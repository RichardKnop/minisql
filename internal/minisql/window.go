package minisql

// WindowFuncKind identifies a window function.
type WindowFuncKind int

// WindowFuncKind constants.
const (
	WindowRowNumber   WindowFuncKind = iota + 1
	WindowRank
	WindowDenseRank
	WindowNtile
	WindowLag
	WindowLead
	WindowFirstValue
	WindowLastValue
	WindowNthValue
	WindowSum
	WindowAvg
	WindowCount
	WindowMin
	WindowMax
)

// FrameMode describes the window frame unit.
type FrameMode int

// FrameMode constants.
const (
	FrameRows  FrameMode = iota + 1 // ROWS BETWEEN ...
	FrameRange                      // RANGE BETWEEN ...
)

// FrameBoundKind describes one side of a window frame boundary.
type FrameBoundKind int

// FrameBoundKind constants.
const (
	FrameUnboundedPreceding FrameBoundKind = iota + 1
	FramePreceding                         // N PRECEDING
	FrameCurrentRow
	FrameFollowing // N FOLLOWING
	FrameUnboundedFollowing
)

// FrameBound is one side of a window frame specification.
type FrameBound struct {
	Kind   FrameBoundKind
	Offset int // only meaningful for FramePreceding / FrameFollowing
}

// WindowFrame is the optional ROWS/RANGE BETWEEN ... AND ... specification.
type WindowFrame struct {
	Mode  FrameMode
	Start FrameBound
	End   FrameBound
}

// WindowSpec is the OVER (...) clause attached to a window function call.
type WindowSpec struct {
	PartitionBy []string    // column names
	OrderBy     []OrderBy   // ORDER BY list
	Frame       *WindowFrame // nil means use default frame for the function
}

// WindowFunc holds the window-function kind and its OVER clause.
// It is embedded inside an Expr when the Expr represents a windowed call.
type WindowFunc struct {
	Kind WindowFuncKind
	// Argument is the expression passed to the function (col reference for
	// value functions; the N argument for NTILE/NTH_VALUE/LAG/LEAD).
	Arg    *Expr
	Arg2   *Expr // second optional argument (default value for LAG/LEAD; N for NTH_VALUE)
	Spec   WindowSpec
}
