package minisql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Interval represents a SQL interval with two components:
//
//   - Months: calendar months (years×12 + months); variable-length because a month
//     contains a different number of days depending on which month it is.
//   - Micros: fixed-duration part in microseconds (weeks×7×86400×1e6 + days×86400×1e6 + …).
//
// Separating months from the fixed-duration component mirrors PostgreSQL's internal
// representation: INTERVAL '1 month' cannot be expressed as a fixed number of
// microseconds, so it must be handled via calendar arithmetic.
type Interval struct {
	Months int32 // total months component (years×12 + months)
	Micros int64 // fixed-duration component in microseconds
}

// String returns a human-readable SQL representation, e.g. "INTERVAL '3 days 4 hours'".
func (iv Interval) String() string {
	if iv.Months == 0 && iv.Micros == 0 {
		return "INTERVAL '0 seconds'"
	}

	var parts []string

	// plural returns the unit name, pluralised when n != 1.
	plural := func(n int64, unit string) string {
		if n == 1 || n == -1 {
			return unit
		}
		return unit + "s"
	}

	// Months component.
	months := iv.Months
	neg := months < 0
	if neg {
		months = -months
	}
	if y := months / 12; y > 0 {
		if neg {
			parts = append(parts, fmt.Sprintf("-%d %s", y, plural(int64(y), "year")))
		} else {
			parts = append(parts, fmt.Sprintf("%d %s", y, plural(int64(y), "year")))
		}
	}
	if m := months % 12; m > 0 {
		if neg {
			parts = append(parts, fmt.Sprintf("-%d %s", m, plural(int64(m), "month")))
		} else {
			parts = append(parts, fmt.Sprintf("%d %s", m, plural(int64(m), "month")))
		}
	}

	// Fixed-duration component.
	micros := iv.Micros
	neg = micros < 0
	if neg {
		micros = -micros
	}
	type durPart struct {
		unit string
		us   int64
	}
	for _, dp := range []durPart{
		{"day", microsecondsInDay},
		{"hour", microsecondsInHour},
		{"minute", microsecondsInMinute},
		{"second", microsecondsInSecond},
		{"microsecond", 1},
	} {
		if v := micros / dp.us; v > 0 {
			micros %= dp.us
			if neg {
				parts = append(parts, fmt.Sprintf("-%d %s", v, plural(v, dp.unit)))
			} else {
				parts = append(parts, fmt.Sprintf("%d %s", v, plural(v, dp.unit)))
			}
		}
	}

	return "INTERVAL '" + strings.Join(parts, " ") + "'"
}

// ParseIntervalString parses an interval specification string such as
// "3 days", "1 year 2 months", "-4 hours 30 minutes".
//
// Supported units (case-insensitive, singular or plural):
//
//	year, month, week, day, hour, minute, second, microsecond
//
// Weeks are converted to days (1 week = 7 days) and stored in Micros.
// Negative values are supported per-component: "-1 day 2 hours".
func ParseIntervalString(s string) (Interval, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Interval{}, fmt.Errorf("empty interval string")
	}

	fields := strings.Fields(s)
	if len(fields) == 0 || len(fields)%2 != 0 {
		return Interval{}, fmt.Errorf("interval must be pairs of 'value unit': %q", s)
	}

	// Accumulate months as int64 to defer the narrowing cast until after a
	// bounds check — satisfies CodeQL "Incorrect conversion between integer types".
	var totalMonths int64
	var iv Interval
	for i := 0; i < len(fields); i += 2 {
		n, err := strconv.ParseInt(fields[i], 10, 64)
		if err != nil {
			return Interval{}, fmt.Errorf("invalid interval value %q: %w", fields[i], err)
		}
		// Normalise unit: lower-case and strip one trailing 's' to handle plurals.
		unit := strings.ToLower(strings.TrimSuffix(fields[i+1], "s"))
		switch unit {
		case "year":
			totalMonths += n * 12
		case "month":
			totalMonths += n
		case "week":
			iv.Micros += n * 7 * microsecondsInDay
		case "day":
			iv.Micros += n * microsecondsInDay
		case "hour":
			iv.Micros += n * microsecondsInHour
		case "minute":
			iv.Micros += n * microsecondsInMinute
		case "second":
			iv.Micros += n * microsecondsInSecond
		case "microsecond":
			iv.Micros += n
		default:
			return Interval{}, fmt.Errorf("unknown interval unit %q", fields[i+1])
		}
	}
	if totalMonths < math.MinInt32 || totalMonths > math.MaxInt32 {
		return Interval{}, fmt.Errorf("interval: total months %d overflows int32", totalMonths)
	}
	iv.Months = int32(totalMonths)
	return iv, nil
}

// AddInterval adds (sign=+1) or subtracts (sign=-1) iv from t.
//
// Month arithmetic is calendar-aware: adding 1 month to Jan 31 yields the last
// day of February (28 or 29 depending on the year). The day component of the
// interval uses fixed 86 400-second days, consistent with PostgreSQL.
func (t Time) AddInterval(iv Interval, sign int32) Time {
	result := t

	// Step 1: apply the months component via calendar arithmetic.
	if iv.Months != 0 {
		totalMonths := int32(t.Year)*12 + int32(t.Month-1) + sign*iv.Months
		// Use Euclidean floor division so negative dates work correctly
		// (Go's % gives a negative remainder for a negative dividend).
		year, monthIdx := intervalFloorDivMod(totalMonths, 12)
		result.Year = year
		result.Month = int8(monthIdx) + 1
		// Clamp day to the last valid day of the new month.
		maxDay := int8(daysInMonth(isLeapYear(int(year)), int(result.Month)))
		if result.Day > maxDay {
			result.Day = maxDay
		}
	}

	// Step 2: apply the fixed-duration component via microsecond arithmetic.
	if iv.Micros != 0 {
		micros := result.TotalMicroseconds() + int64(sign)*iv.Micros
		result = FromMicroseconds(micros)
	}

	return result
}

// intervalFloorDivMod returns (q, r) such that a = q*b + r with 0 ≤ r < b.
// Unlike Go's built-in /, %, it rounds toward negative infinity for negative a.
func intervalFloorDivMod(a, b int32) (int32, int32) {
	q := a / b
	r := a % b
	if r < 0 {
		r += b
		q -= 1
	}
	return q, r
}
