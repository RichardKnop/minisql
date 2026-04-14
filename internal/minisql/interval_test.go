package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── ParseIntervalString ───────────────────────────────────────────────────────

func TestParseIntervalString(t *testing.T) {
	t.Parallel()

	msInDay := int64(microsecondsInDay)
	msInHour := int64(microsecondsInHour)
	msInMin := int64(microsecondsInMinute)
	msInSec := int64(microsecondsInSecond)

	cases := []struct {
		input    string
		expected Interval
	}{
		{"1 year", Interval{Months: 12}},
		{"2 years", Interval{Months: 24}},
		{"3 months", Interval{Months: 3}},
		{"1 year 6 months", Interval{Months: 18}},
		{"1 week", Interval{Micros: 7 * msInDay}},
		{"2 weeks", Interval{Micros: 14 * msInDay}},
		{"3 days", Interval{Micros: 3 * msInDay}},
		{"4 hours", Interval{Micros: 4 * msInHour}},
		{"30 minutes", Interval{Micros: 30 * msInMin}},
		{"45 seconds", Interval{Micros: 45 * msInSec}},
		{"500 microseconds", Interval{Micros: 500}},
		{"-1 day", Interval{Micros: -msInDay}},
		{"-2 months", Interval{Months: -2}},
		{"1 year 2 months 3 days 4 hours 5 minutes 6 seconds",
			Interval{Months: 14, Micros: 3*msInDay + 4*msInHour + 5*msInMin + 6*msInSec}},
	}
	for _, tc := range cases {
		iv, err := ParseIntervalString(tc.input)
		require.NoError(t, err, "input=%q", tc.input)
		assert.Equal(t, tc.expected, iv, "input=%q", tc.input)
	}
}

func TestParseIntervalString_Errors(t *testing.T) {
	t.Parallel()

	cases := []string{
		"",
		"3",        // no unit
		"3 days 5", // trailing value without unit
		"abc days", // non-numeric value
		"1 lightyear",
	}
	for _, s := range cases {
		_, err := ParseIntervalString(s)
		assert.Error(t, err, "input=%q should fail", s)
	}
}

// ── Interval.String ───────────────────────────────────────────────────────────

func TestInterval_String(t *testing.T) {
	t.Parallel()

	day := int64(microsecondsInDay)
	hour := int64(microsecondsInHour)

	cases := []struct {
		iv       Interval
		expected string
	}{
		{Interval{}, "INTERVAL '0 seconds'"},
		{Interval{Months: 12}, "INTERVAL '1 year'"},
		{Interval{Months: 24}, "INTERVAL '2 years'"},
		{Interval{Months: 3}, "INTERVAL '3 months'"},
		{Interval{Months: 14}, "INTERVAL '1 year 2 months'"},
		{Interval{Micros: day}, "INTERVAL '1 day'"},
		{Interval{Micros: 3 * day}, "INTERVAL '3 days'"},
		{Interval{Micros: hour}, "INTERVAL '1 hour'"},
		{Interval{Micros: -day}, "INTERVAL '-1 day'"},
		{Interval{Months: -6}, "INTERVAL '-6 months'"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.expected, tc.iv.String(), "iv=%+v", tc.iv)
	}
}

// ── Time.AddInterval: fixed-duration (days, hours, minutes, seconds) ──────────

func TestAddInterval_Days(t *testing.T) {
	t.Parallel()

	ts := MustParseTimestamp("2024-03-15 10:00:00")
	result := ts.AddInterval(Interval{Micros: 3 * microsecondsInDay}, 1)
	assert.Equal(t, MustParseTimestamp("2024-03-18 10:00:00"), result)
}

func TestAddInterval_SubtractDays(t *testing.T) {
	t.Parallel()

	ts := MustParseTimestamp("2024-03-15 10:00:00")
	result := ts.AddInterval(Interval{Micros: 5 * microsecondsInDay}, -1)
	assert.Equal(t, MustParseTimestamp("2024-03-10 10:00:00"), result)
}

func TestAddInterval_Hours(t *testing.T) {
	t.Parallel()

	ts := MustParseTimestamp("2024-06-01 22:00:00")
	result := ts.AddInterval(Interval{Micros: 3 * microsecondsInHour}, 1)
	assert.Equal(t, MustParseTimestamp("2024-06-02 01:00:00"), result) // crosses midnight
}

func TestAddInterval_Minutes(t *testing.T) {
	t.Parallel()

	ts := MustParseTimestamp("2024-01-01 00:00:00")
	result := ts.AddInterval(Interval{Micros: 90 * microsecondsInMinute}, 1)
	assert.Equal(t, MustParseTimestamp("2024-01-01 01:30:00"), result)
}

// ── Time.AddInterval: month arithmetic (calendar-aware) ───────────────────────

func TestAddInterval_OneMonth_NoClamping(t *testing.T) {
	t.Parallel()

	ts := MustParseTimestamp("2024-01-15 00:00:00")
	result := ts.AddInterval(Interval{Months: 1}, 1)
	assert.Equal(t, MustParseTimestamp("2024-02-15 00:00:00"), result)
}

func TestAddInterval_OneMonth_ClampToFeb28(t *testing.T) {
	t.Parallel()

	// 2023 is not a leap year — Jan 31 + 1 month → Feb 28.
	ts := MustParseTimestamp("2023-01-31 00:00:00")
	result := ts.AddInterval(Interval{Months: 1}, 1)
	assert.Equal(t, MustParseTimestamp("2023-02-28 00:00:00"), result)
}

func TestAddInterval_OneMonth_ClampToFeb29LeapYear(t *testing.T) {
	t.Parallel()

	// 2024 is a leap year — Jan 31 + 1 month → Feb 29.
	ts := MustParseTimestamp("2024-01-31 00:00:00")
	result := ts.AddInterval(Interval{Months: 1}, 1)
	assert.Equal(t, MustParseTimestamp("2024-02-29 00:00:00"), result)
}

func TestAddInterval_OneYear(t *testing.T) {
	t.Parallel()

	ts := MustParseTimestamp("2024-03-15 00:00:00")
	result := ts.AddInterval(Interval{Months: 12}, 1)
	assert.Equal(t, MustParseTimestamp("2025-03-15 00:00:00"), result)
}

func TestAddInterval_YearEndCarryover(t *testing.T) {
	t.Parallel()

	// Dec + 1 month → Jan of next year.
	ts := MustParseTimestamp("2024-12-15 00:00:00")
	result := ts.AddInterval(Interval{Months: 1}, 1)
	assert.Equal(t, MustParseTimestamp("2025-01-15 00:00:00"), result)
}

func TestAddInterval_SubtractMonth(t *testing.T) {
	t.Parallel()

	ts := MustParseTimestamp("2024-03-31 00:00:00")
	result := ts.AddInterval(Interval{Months: 1}, -1) // Mar 31 - 1 month → Feb 29 (leap year)
	assert.Equal(t, MustParseTimestamp("2024-02-29 00:00:00"), result)
}

func TestAddInterval_SubtractMonthYearBoundary(t *testing.T) {
	t.Parallel()

	// Jan - 1 month → Dec of previous year.
	ts := MustParseTimestamp("2024-01-15 00:00:00")
	result := ts.AddInterval(Interval{Months: 1}, -1)
	assert.Equal(t, MustParseTimestamp("2023-12-15 00:00:00"), result)
}

// ── Time.AddInterval: compound (months + fixed-duration) ─────────────────────

func TestAddInterval_Compound(t *testing.T) {
	t.Parallel()

	// 2024-01-31 + 1 month 1 day → Feb 29 (clamp) + 1 day → Mar 1.
	ts := MustParseTimestamp("2024-01-31 00:00:00")
	iv := Interval{Months: 1, Micros: microsecondsInDay}
	result := ts.AddInterval(iv, 1)
	assert.Equal(t, MustParseTimestamp("2024-03-01 00:00:00"), result)
}

// ── Timestamp - Timestamp via Expr.Eval ──────────────────────────────────────

func TestExpr_TimestampMinusTimestamp(t *testing.T) {
	t.Parallel()

	ts1 := MustParseTimestamp("2024-01-04 00:00:00")
	ts2 := MustParseTimestamp("2024-01-01 00:00:00")
	expr := &Expr{
		Left:  &Expr{Literal: ts1},
		Right: &Expr{Literal: ts2},
		Op:    ArithSub,
	}
	val, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.Equal(t, Interval{Micros: 3 * microsecondsInDay}, val)
}

// ── Interval ± Interval via Expr.Eval ────────────────────────────────────────

func TestExpr_IntervalPlusInterval(t *testing.T) {
	t.Parallel()

	iv1 := Interval{Months: 1}
	iv2 := Interval{Micros: microsecondsInDay}
	expr := &Expr{
		Left:  &Expr{Literal: iv1},
		Right: &Expr{Literal: iv2},
		Op:    ArithAdd,
	}
	val, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.Equal(t, Interval{Months: 1, Micros: microsecondsInDay}, val)
}

func TestExpr_IntervalMinusInterval(t *testing.T) {
	t.Parallel()

	iv1 := Interval{Micros: 5 * microsecondsInDay}
	iv2 := Interval{Micros: 2 * microsecondsInDay}
	expr := &Expr{
		Left:  &Expr{Literal: iv1},
		Right: &Expr{Literal: iv2},
		Op:    ArithSub,
	}
	val, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.Equal(t, Interval{Micros: 3 * microsecondsInDay}, val)
}
