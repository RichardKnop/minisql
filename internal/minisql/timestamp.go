package minisql

import (
	"fmt"
	"strconv"
	"strings"
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

// Time is a custom type implementing PostgreSQL like TIMESTAMP type
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

func FromMicroseconds(microseconds int64) Time {
	if microseconds == 0 {
		return Time{
			Year:         2000,
			Month:        1,
			Day:          1,
			Hour:         0,
			Minutes:      0,
			Seconds:      0,
			Microseconds: 0,
		}
	}

	var t Time
	if microseconds > 0 {
		// Forward from 2000-01-01
		absMicroseconds := microseconds
		year := int32(2000)
		for {
			leapYear := isLeapYear(int(year))
			var yearMicroseconds int64
			if leapYear {
				yearMicroseconds = microsecondsInYear + microsecondsInDay
			} else {
				yearMicroseconds = microsecondsInYear
			}
			if absMicroseconds >= yearMicroseconds {
				absMicroseconds -= yearMicroseconds
				year += 1
			} else {
				break
			}
		}
		t.Year = year

		leapYear := isLeapYear(int(t.Year))
		month := 1
		for {
			daysInThisMonth := daysInMonth(leapYear, month)
			monthMicroseconds := int64(daysInThisMonth) * microsecondsInDay
			if absMicroseconds >= monthMicroseconds {
				absMicroseconds -= monthMicroseconds
				month += 1
			} else {
				break
			}
		}
		t.Month = int8(month)

		t.Day = int8(absMicroseconds/microsecondsInDay) + 1
		absMicroseconds = absMicroseconds % microsecondsInDay

		t.Hour = int8(absMicroseconds / microsecondsInHour)
		absMicroseconds = absMicroseconds % microsecondsInHour

		t.Minutes = int8(absMicroseconds / microsecondsInMinute)
		absMicroseconds = absMicroseconds % microsecondsInMinute

		t.Seconds = int8(absMicroseconds / microsecondsInSecond)
		absMicroseconds = absMicroseconds % microsecondsInSecond

		t.Microseconds = int32(absMicroseconds)
		return t
	}

	// Backward from 2000-01-01
	absMicroseconds := -microseconds
	year := int32(1999)
	for {
		leapYear := isLeapYear(int(year))
		var yearMicroseconds int64
		if leapYear {
			yearMicroseconds = microsecondsInYear + microsecondsInDay
		} else {
			yearMicroseconds = microsecondsInYear
		}
		if absMicroseconds >= yearMicroseconds {
			absMicroseconds -= yearMicroseconds
			if absMicroseconds > 0 {
				year -= 1
			}
		} else {
			break
		}
	}
	t.Year = year

	if absMicroseconds == 0 {
		t.Month = 1
		t.Day = 1
		t.Hour = 0
		t.Minutes = 0
		t.Seconds = 0
		t.Microseconds = 0
		return t
	}

	leapYear := isLeapYear(int(t.Year))
	month := 12
	for {
		daysInThisMonth := daysInMonth(leapYear, month)
		monthMicroseconds := int64(daysInThisMonth) * microsecondsInDay
		if absMicroseconds >= monthMicroseconds {
			absMicroseconds -= monthMicroseconds
			month -= 1
		} else {
			break
		}
	}
	t.Month = int8(month)

	// Days, hours, minutes, seconds, microseconds are counted from the end of the month backwards
	daysInThisMonth := daysInMonth(leapYear, month)
	if absMicroseconds == microsecondsInDay {
		t.Day = int8(daysInThisMonth)
		return t
	}
	t.Day = int8(daysInThisMonth - int(absMicroseconds/microsecondsInDay))
	absMicroseconds = absMicroseconds % microsecondsInDay

	t.Hour = int8(23 - int(absMicroseconds/microsecondsInHour))
	absMicroseconds = absMicroseconds % microsecondsInHour

	t.Minutes = int8(59 - int(absMicroseconds/microsecondsInMinute))
	absMicroseconds = absMicroseconds % microsecondsInMinute

	t.Seconds = int8(59 - int(absMicroseconds/microsecondsInSecond))
	absMicroseconds = absMicroseconds % microsecondsInSecond

	t.Microseconds = int32(1_000_000 - int(absMicroseconds))
	if t.Microseconds == 1_000_000 {
		t.Microseconds = 0
	}
	return t
}

// Microseconds count since 2000-01-01 00:00:00 UTC
// Negative values represent dates before 2000-01-01 00:00:00 UTC
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

func MustParseTimestamp(timestampStr string) Time {
	t, err := ParseTimestamp(timestampStr)
	if err != nil {
		panic(err)
	}
	return t
}

func ParseTimestamp(timestampStr string) (Time, error) {
	if len(timestampStr) == 0 {
		return Time{}, fmt.Errorf("empty timestamp string")
	}
	if len(timestampStr) < miniumTimestampLength {
		return Time{}, fmt.Errorf("timestamp string too short: %s", timestampStr)
	}
	if len(timestampStr) > maximumTimestampLength {
		return Time{}, fmt.Errorf("timestamp string too long: %s", timestampStr)
	}
	bc := false
	if strings.HasSuffix(timestampStr, " BC") {
		bc = true
		timestampStr = strings.TrimSuffix(timestampStr, " BC")
	}

	parts := strings.Split(timestampStr, " ")
	if len(parts) != 2 {
		return Time{}, fmt.Errorf("timestamp string invalid format (parts): %s", timestampStr)
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
		microseconds, err = strconv.Atoi(secParts[1])
		if err != nil {
			return Time{}, fmt.Errorf("timestamp string invalid format (microseconds part): %s", timestampStr)
		}
	}

	// Validate time
	if err := isValidTime(hours, minutes, seconds, microseconds); err != nil {
		return Time{}, fmt.Errorf("invalid time in timestamp %s: %w", timestampStr, err)
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
		return fmt.Errorf("invalid hour")
	}
	if minutes < 0 || minutes > 59 {
		return fmt.Errorf("invalid minutes")
	}
	if seconds < 0 || seconds > 59 {
		return fmt.Errorf("invalid seconds")
	}
	if microseconds < 0 || microseconds > 999999 {
		return fmt.Errorf("invalid microseconds")
	}
	return nil
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
