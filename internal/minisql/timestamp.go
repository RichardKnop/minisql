package minisql

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	timestampFormat = "2006-01-02 15:04:05.999999"
	// 2024-06-15 12:34:56
	miniumTimestampLength = 19
	// 4713-06-15 12:34:56.123456 BC + 3 (for " BC")
	maximumTimestampLength = 29

	lowYear  = -4712 // astronomical year 4713 BC
	highYear = 294276

	microsecondsInSecond = 1_000_000
	microsecondsInMinute = 60 * microsecondsInSecond
	microsecondsInHour   = 60 * microsecondsInMinute
	microsecondsInDay    = 24 * microsecondsInHour
	microsecondsInYear   = 365 * microsecondsInDay
	maxFractionalDigits  = 6
)

var (
	leapYearsBefore = make([]int32, 0, 4713+2000+1)
	leapYearsAfter  = make([]int32, 0, 294276-2000+1)
)

func init() {
	for year := 1999; year >= lowYear; year-- {
		if isLeapYear(year) {
			leapYearsBefore = append(leapYearsBefore, int32(year))
		}
	}
	for year := 2000; year <= highYear; year++ {
		if isLeapYear(year) {
			leapYearsAfter = append(leapYearsAfter, int32(year))
		}
	}
}

// Time is a custom type implementing a PostgreSQL-like TIMESTAMP type without timezone.
// low value 4713 BC
// high value 294276 AD
type Time struct {
	Year         int32 // astonomical year numbering, e.g. 1 BC = year 0, 2 BC = -1, etc.
	Month        int8
	Day          int8
	Hour         int8
	Minutes      int8
	Seconds      int8
	Microseconds int32
}

// GoTime converts the custom Time value to a standard library time.Time.
func (t Time) GoTime() time.Time {
	return time.Date(
		int(t.Year),
		time.Month(t.Month),
		int(t.Day),
		int(t.Hour),
		int(t.Minutes),
		int(t.Seconds),
		int(t.Microseconds)*1000,
		time.UTC,
	)
}

func (t Time) String() string {
	bc := ""
	year := t.Year
	if year <= 0 {
		bc = " BC"
		year = -(year - 1)
	}
	if t.Microseconds == 0 {
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d%s",
			year, t.Month, t.Day,
			t.Hour, t.Minutes, t.Seconds,
			bc,
		)
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d%s",
		year, t.Month, t.Day,
		t.Hour, t.Minutes, t.Seconds, t.Microseconds,
		bc,
	)
}

// FromMicroseconds constructs a Time from microseconds since 2000-01-01 00:00:00 UTC.
func FromMicroseconds(microseconds int64) Time {
	days, microsecondsOfDay := splitMicrosecondsIntoDays(microseconds)
	year, month, day := dateFromEpochDays(days)

	return Time{
		Year:         year,
		Month:        int8(month),
		Day:          int8(day),
		Hour:         int8(microsecondsOfDay / microsecondsInHour),
		Minutes:      int8((microsecondsOfDay % microsecondsInHour) / microsecondsInMinute),
		Seconds:      int8((microsecondsOfDay % microsecondsInMinute) / microsecondsInSecond),
		Microseconds: int32(microsecondsOfDay % microsecondsInSecond),
	}
}

// TotalMicroseconds returns the microsecond count since 2000-01-01 00:00:00 UTC.
// Negative values represent dates before 2000-01-01 00:00:00 UTC.
func (t Time) TotalMicroseconds() int64 {
	var (
		leapYear = isLeapYear(int(t.Year))
		total    int64
	)

	if t.Year >= 2000 {
		if t.Year > 2000 {
			leapYearsCount := 0
			for _, ly := range leapYearsAfter {
				if ly >= int32(t.Year) {
					break
				}
				leapYearsCount += 1
			}
			total += int64(t.Year-2000) * microsecondsInYear
			total += int64(leapYearsCount) * microsecondsInDay
		}

		for i := 1; i < int(t.Month); i++ {
			total += int64(daysInMonth(leapYear, i)) * microsecondsInDay
		}
		if t.Day > 1 {
			total += int64(t.Day-1) * microsecondsInDay
		}
		if t.Hour > 0 {
			total += int64(t.Hour) * microsecondsInHour
		}
		if t.Minutes > 0 {
			total += int64(t.Minutes) * microsecondsInMinute
		}
		total += int64(t.Seconds) * 1_000_000
		total += int64(t.Microseconds)
		return total
	}

	leapYearsCount := 0
	for _, ly := range leapYearsBefore {
		if ly <= int32(t.Year) {
			break
		}
		leapYearsCount += 1
	}
	total -= int64(1999-t.Year) * microsecondsInYear
	total -= int64(leapYearsCount) * microsecondsInDay

	for i := 12; i > int(t.Month); i-- {
		total -= int64(daysInMonth(leapYear, i)) * microsecondsInDay
	}
	if int(t.Day) < daysInMonth(leapYear, int(t.Month)) {
		total -= int64(daysInMonth(leapYear, int(t.Month))-int(t.Day)) * microsecondsInDay
	}
	if int(t.Hour) < 23 {
		total -= int64(23-int(t.Hour)) * microsecondsInHour
	}
	if int(t.Minutes) < 59 {
		total -= int64(59-int(t.Minutes)) * microsecondsInMinute
	}
	total -= int64(59-int(t.Seconds)) * 1_000_000
	total -= int64(1_000_000 - int(t.Microseconds))
	return total
}

// MustParseTimestamp parses a timestamp string and panics on error.
func MustParseTimestamp(timestampStr string) Time {
	t, err := ParseTimestamp(timestampStr)
	if err != nil {
		panic(err)
	}
	return t
}

// ParseTimestamp parses a PostgreSQL-style timestamp string into a Time value.
func ParseTimestamp(timestampStr string) (Time, error) {
	if timestampStr == "" {
		return Time{}, errors.New("empty timestamp string")
	}
	if len(timestampStr) < miniumTimestampLength {
		return Time{}, fmt.Errorf("timestamp string too short: %s", timestampStr)
	}
	bc := false
	if strings.HasSuffix(timestampStr, " BC") {
		bc = true
		timestampStr = strings.TrimSuffix(timestampStr, " BC")
	}

	parts := strings.Fields(timestampStr)
	if len(parts) == 3 && isTimezoneToken(parts[2]) {
		return Time{}, fmt.Errorf("timestamp with timezone is not supported: %s %s", strings.Join(parts[:2], " "), parts[2])
	}
	if len(parts) != 2 {
		return Time{}, fmt.Errorf("timestamp string invalid format (parts): %s", timestampStr)
	}
	if containsTimezoneMarker(parts[1]) {
		return Time{}, fmt.Errorf("timestamp with timezone is not supported: %s", timestampStr)
	}
	if len(timestampStr) > maximumTimestampLength {
		return Time{}, fmt.Errorf("timestamp string too long: %s", timestampStr)
	}

	// Parse date parts
	dateParts := strings.Split(parts[0], "-")
	if len(dateParts) != 3 {
		return Time{}, fmt.Errorf("timestamp string invalid format (date parts): %s", timestampStr)
	}
	year, err := strconv.Atoi(dateParts[0])
	if err != nil {
		return Time{}, fmt.Errorf("timestamp string invalid format (year part): %s", timestampStr)
	}
	month, err := strconv.Atoi(dateParts[1])
	if err != nil {
		return Time{}, fmt.Errorf("timestamp string invalid format (month part): %s", timestampStr)
	}
	day, err := strconv.Atoi(dateParts[2])
	if err != nil {
		return Time{}, fmt.Errorf("timestamp string invalid format (day part): %s", timestampStr)
	}

	// Validate date
	if year == 0 {
		return Time{}, fmt.Errorf("there is no year 0 in gregorian calendar: %s", timestampStr)
	}
	// Convert to astronomical year numbering for leap year calculation,
	// ie. 1 BC = year 0, 2 BC = -1, etc.
	if bc {
		year = -year + 1
	}
	if year < lowYear {
		return Time{}, fmt.Errorf("year < -4713 in timestamp: %s BC", timestampStr)
	}
	if year > highYear {
		return Time{}, fmt.Errorf("year > 294276 in timestamp: %s", timestampStr)
	}

	leapYear := isLeapYear(year)
	if err := isValidDate(leapYear, month, day); err != nil {
		return Time{}, fmt.Errorf("%w: %s", err, timestampStr)
	}

	// Parse time parts
	timeParts := strings.Split(parts[1], ":")
	if len(timeParts) != 3 {
		return Time{}, fmt.Errorf("timestamp string invalid format (time parts): %s", timestampStr)
	}
	hours, err := strconv.Atoi(timeParts[0])
	if err != nil {
		return Time{}, fmt.Errorf("timestamp string invalid format (hours part): %s", timestampStr)
	}
	minutes, err := strconv.Atoi(timeParts[1])
	if err != nil {
		return Time{}, fmt.Errorf("timestamp string invalid format (minutes part): %s", timestampStr)
	}
	secParts := strings.Split(timeParts[2], ".")
	if len(secParts) < 1 || len(secParts) > 2 {
		return Time{}, fmt.Errorf("timestamp string invalid format (seconds float): %s", timestampStr)
	}
	seconds, err := strconv.Atoi(secParts[0])
	if err != nil {
		return Time{}, fmt.Errorf("timestamp string invalid format (seconds part): %s", timestampStr)
	}
	var microseconds int
	if len(secParts) == 2 {
		microseconds, err = parseFractionalMicroseconds(secParts[1])
		if err != nil {
			return Time{}, fmt.Errorf("timestamp string invalid format (microseconds part): %s", timestampStr)
		}
	}

	// Validate time
	if err := isValidTime(hours, minutes, seconds, microseconds); err != nil {
		return Time{}, fmt.Errorf("invalid time in timestamp %s: %w", timestampStr, err)
	}

	// Explicit range guards so static analysis can verify the narrowing casts below are safe.
	// These ranges are a superset of the domain-valid ranges enforced by isValidDate/isValidTime
	// above, so they will never trigger for well-formed input.
	if year < math.MinInt32 || year > math.MaxInt32 {
		return Time{}, fmt.Errorf("year %d overflows int32", year)
	}
	for name, v := range map[string]int{"month": month, "day": day, "hour": hours, "minute": minutes, "second": seconds} {
		if v < math.MinInt8 || v > math.MaxInt8 {
			return Time{}, fmt.Errorf("%s %d overflows int8", name, v)
		}
	}
	if microseconds < 0 || microseconds > math.MaxInt32 {
		return Time{}, fmt.Errorf("microseconds %d overflows int32", microseconds)
	}

	return Time{
		Year:         int32(year),
		Month:        int8(month),
		Day:          int8(day),
		Hour:         int8(hours),
		Minutes:      int8(minutes),
		Seconds:      int8(seconds),
		Microseconds: int32(microseconds),
	}, nil
}

func isValidDate(leapYear bool, month, day int) error {
	if month < 1 || month > 12 {
		return fmt.Errorf("invalid month: %d", month)
	}
	if day < 1 {
		return fmt.Errorf("invalid day: %d", day)
	}
	if day <= daysInMonth(leapYear, month) {
		return nil
	}
	return fmt.Errorf("invalid day: %d for month: %d", day, month)
}

func isValidTime(hour, minutes, seconds, microseconds int) error {
	if hour < 0 || hour > 23 {
		return errors.New("invalid hour")
	}
	if minutes < 0 || minutes > 59 {
		return errors.New("invalid minutes")
	}
	if seconds < 0 || seconds > 59 {
		return errors.New("invalid seconds")
	}
	if microseconds < 0 || microseconds > 999999 {
		return errors.New("invalid microseconds")
	}
	return nil
}

func parseFractionalMicroseconds(part string) (int, error) {
	if part == "" || len(part) > maxFractionalDigits {
		return 0, errors.New("invalid microseconds")
	}

	value, err := strconv.Atoi(part)
	if err != nil {
		return 0, err
	}

	for i := len(part); i < maxFractionalDigits; i++ {
		value *= 10
	}

	return value, nil
}

func splitMicrosecondsIntoDays(microseconds int64) (int64, int64) {
	days := microseconds / microsecondsInDay
	remainder := microseconds % microsecondsInDay
	if remainder < 0 {
		days -= 1
		remainder += microsecondsInDay
	}
	return days, remainder
}

func dateFromEpochDays(days int64) (int32, int, int) {
	if days >= 0 {
		return dateFromEpochDaysForward(days)
	}
	return dateFromEpochDaysBackward(days)
}

func dateFromEpochDaysForward(days int64) (int32, int, int) {
	year := int32(2000)
	for {
		yearDays := int64(daysInYear(int(year)))
		if days < yearDays {
			break
		}
		days -= yearDays
		year += 1
	}

	month := 1
	for {
		monthDays := int64(daysInMonth(isLeapYear(int(year)), month))
		if days < monthDays {
			break
		}
		days -= monthDays
		month += 1
	}

	return year, month, int(days) + 1
}

func dateFromEpochDaysBackward(days int64) (int32, int, int) {
	remainingDays := -days
	year := int32(1999)
	for {
		yearDays := int64(daysInYear(int(year)))
		if remainingDays <= yearDays {
			break
		}
		remainingDays -= yearDays
		year--
	}

	month := 12
	for {
		monthDays := int64(daysInMonth(isLeapYear(int(year)), month))
		if remainingDays <= monthDays {
			break
		}
		remainingDays -= monthDays
		month--
	}

	day := daysInMonth(isLeapYear(int(year)), month) - int(remainingDays) + 1
	return year, month, day
}

func containsTimezoneMarker(timePart string) bool {
	if strings.HasSuffix(timePart, "Z") || strings.HasSuffix(timePart, "z") {
		return true
	}
	return strings.ContainsAny(timePart, "+-")
}

func daysInYear(year int) int {
	if isLeapYear(year) {
		return 366
	}
	return 365
}

func isTimezoneToken(token string) bool {
	upper := strings.ToUpper(token)
	if upper == "Z" || upper == "UTC" || upper == "GMT" {
		return true
	}
	return strings.ContainsAny(token, "+-")
}

// isLeapYear returns true if the given year is a leap year
// according to the proleptic Gregorian calendar (Postgres behavior).
// For the Gregorian calendar (used by Postgres for all years), a year is a leap year if:
// - It is divisible by 4,
// - Except years divisible by 100, unless also divisible by 400.
func isLeapYear(year int) bool {
	if year%4 != 0 {
		return false
	}
	if year%100 == 0 && year%400 != 0 {
		return false
	}
	return true
}

// daysInMonth returns the number of days in a given month for a given year.
func daysInMonth(leapYear bool, month int) int {
	switch month {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31
	case 4, 6, 9, 11:
		return 30
	case 2:
		if leapYear {
			return 29
		}
		return 28
	default:
		return 0 // invalid month
	}
}
