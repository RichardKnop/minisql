package minisql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		timestampStr  string
		expected      Time
		expectedTotal int64
		err           error
	}{
		{
			"invalid timestamp",
			"invalid-timestamp",
			Time{},
			0,
			fmt.Errorf("timestamp string too short: invalid-timestamp"),
		},
		{
			"timestamp below minimum",
			"4714-01-01 00:00:00.000000 BC",
			Time{},
			0,
			fmt.Errorf("year < -4713 in timestamp: 4714-01-01 00:00:00.000000 BC"),
		},
		{
			"timestamp beyond maximum",
			"294277-01-01 00:00:00.000000",
			Time{},
			0,
			fmt.Errorf("year > 294276 in timestamp: 294277-01-01 00:00:00.000000"),
		},
		{
			"0 is not a valid year in gregorian calendar",
			"0000-01-01 00:00:00.000000",
			Time{},
			0,
			fmt.Errorf("there is no year 0 in gregorian calendar: 0000-01-01 00:00:00.000000"),
		},
		{
			"earliest valid timestamp",
			"4713-01-01 00:00:00 BC",
			Time{
				Year:         -4712,
				Month:        1,
				Day:          1,
				Hour:         0,
				Minutes:      0,
				Seconds:      0,
				Microseconds: 0,
			},
			-(2000+4712)*microsecondsInYear - leapYearsBetween(-4712, 2000)*microsecondsInDay,
			nil,
		},
		{
			"1 BC - astronomical year 0 is a leap year",
			"0001-01-01 00:00:00 BC",
			Time{
				Year:         0,
				Month:        1,
				Day:          1,
				Hour:         0,
				Minutes:      0,
				Seconds:      0,
				Microseconds: 0,
			},
			-2000*microsecondsInYear - leapYearsBetween(0, 2000)*microsecondsInDay,
			nil,
		},
		{
			"1ms before Year 1 AD",
			"0001-12-31 23:59:59.999999 BC",
			Time{
				Year:         0,
				Month:        12,
				Day:          31,
				Hour:         23,
				Minutes:      59,
				Seconds:      59,
				Microseconds: 999999,
			},
			-1999*microsecondsInYear - leapYearsBetween(1, 2000)*microsecondsInDay - 1,
			nil,
		},
		{
			"Year 1 AD",
			"0001-01-01 00:00:00",
			Time{
				Year:         1,
				Month:        1,
				Day:          1,
				Hour:         0,
				Minutes:      0,
				Seconds:      0,
				Microseconds: 0,
			},
			-1999*microsecondsInYear - leapYearsBetween(1, 2000)*microsecondsInDay,
			nil,
		},
		{
			"1900 is not a leap year",
			"1900-01-01 00:00:00",
			Time{
				Year:         1900,
				Month:        1,
				Day:          1,
				Hour:         0,
				Minutes:      0,
				Seconds:      0,
				Microseconds: 0,
			},
			-100*microsecondsInYear - 24*microsecondsInDay,
			nil,
		},
		{
			"Leap year february 29th",
			"1996-02-29 00:00:00",
			Time{
				Year:         1996,
				Month:        2,
				Day:          29,
				Hour:         0,
				Minutes:      0,
				Seconds:      0,
				Microseconds: 0,
			},
			-3*microsecondsInYear - (365-30-28)*microsecondsInDay,
			nil,
		},
		{
			"4 years before epoch start including a leap year",
			"1996-01-01 00:00:00",
			Time{
				Year:         1996,
				Month:        1,
				Day:          1,
				Hour:         0,
				Minutes:      0,
				Seconds:      0,
				Microseconds: 0,
			},
			-4*microsecondsInYear - microsecondsInDay,
			nil,
		},
		{
			"Non leap year february 29th invalid timestamp",
			"1999-02-29 12:34:56.123456",
			Time{
				Year:         1999,
				Month:        2,
				Day:          29,
				Hour:         12,
				Minutes:      34,
				Seconds:      56,
				Microseconds: 123456,
			},
			762525296123456,
			fmt.Errorf("invalid day: 29 for month: 2: 1999-02-29 12:34:56.123456"),
		},
		{
			"One year before epoch start",
			"1999-01-01 00:00:00",
			Time{
				Year:         1999,
				Month:        1,
				Day:          1,
				Hour:         0,
				Minutes:      0,
				Seconds:      0,
				Microseconds: 0,
			},
			-microsecondsInYear,
			nil,
		},
		{
			"1m 1d 1h 1min 1s 1ms before epoch start",
			"1999-11-29 22:58:58.999999",
			Time{
				Year:         1999,
				Month:        11,
				Day:          29,
				Hour:         22,
				Minutes:      58,
				Seconds:      58,
				Microseconds: 999999,
			},
			-31*microsecondsInDay - microsecondsInDay - microsecondsInHour - microsecondsInMinute - microsecondsInSecond - 1,
			nil,
		},
		{
			"1d 1h 1min 1s 1ms before epoch start",
			"1999-12-30 22:58:58.999999",
			Time{
				Year:         1999,
				Month:        12,
				Day:          30,
				Hour:         22,
				Minutes:      58,
				Seconds:      58,
				Microseconds: 999999,
			},
			-microsecondsInDay - microsecondsInHour - microsecondsInMinute - microsecondsInSecond - 1,
			nil,
		},
		{
			"1h 1min 1s 1ms before epoch start",
			"1999-12-31 22:58:58.999999",
			Time{
				Year:         1999,
				Month:        12,
				Day:          31,
				Hour:         22,
				Minutes:      58,
				Seconds:      58,
				Microseconds: 999999,
			},
			-microsecondsInHour - microsecondsInMinute - microsecondsInSecond - 1,
			nil,
		},
		{
			"1min 1s 1ms before epoch start",
			"1999-12-31 23:58:58.999999",
			Time{
				Year:         1999,
				Month:        12,
				Day:          31,
				Hour:         23,
				Minutes:      58,
				Seconds:      58,
				Microseconds: 999999,
			},
			-microsecondsInMinute - microsecondsInSecond - 1,
			nil,
		},
		{
			"1s 1ms before epoch start",
			"1999-12-31 23:59:58.999999",
			Time{
				Year:         1999,
				Month:        12,
				Day:          31,
				Hour:         23,
				Minutes:      59,
				Seconds:      58,
				Microseconds: 999999,
			},
			-microsecondsInSecond - 1,
			nil,
		},
		{
			"1ms before epoch start",
			"1999-12-31 23:59:59.999999",
			Time{
				Year:         1999,
				Month:        12,
				Day:          31,
				Hour:         23,
				Minutes:      59,
				Seconds:      59,
				Microseconds: 999999,
			},
			-1,
			nil,
		},
		{
			"Exactly epoch start",
			"2000-01-01 00:00:00",
			Time{
				Year:         2000,
				Month:        1,
				Day:          1,
				Hour:         0,
				Minutes:      0,
				Seconds:      0,
				Microseconds: 0,
			},
			0,
			nil,
		},
		{
			"1ms after epoch start",
			"2000-01-01 00:00:00.000001",
			Time{
				Year:         2000,
				Month:        1,
				Day:          1,
				Hour:         0,
				Minutes:      0,
				Seconds:      0,
				Microseconds: 1,
			},
			1,
			nil,
		},
		{
			"1s 1ms after epoch start",
			"2000-01-01 00:00:01.000001",
			Time{
				Year:         2000,
				Month:        1,
				Day:          1,
				Hour:         0,
				Minutes:      0,
				Seconds:      1,
				Microseconds: 1,
			},
			1_000_001,
			nil,
		},
		{
			"1min 1s 1ms after epoch start",
			"2000-01-01 00:01:01.000001",
			Time{
				Year:         2000,
				Month:        1,
				Day:          1,
				Hour:         0,
				Minutes:      1,
				Seconds:      1,
				Microseconds: 1,
			},
			microsecondsInMinute + 1_000_001,
			nil,
		},
		{
			"1h 1min 1s 1ms after epoch start",
			"2000-01-01 01:01:01.000001",
			Time{
				Year:         2000,
				Month:        1,
				Day:          1,
				Hour:         1,
				Minutes:      1,
				Seconds:      1,
				Microseconds: 1,
			},
			microsecondsInHour + microsecondsInMinute + 1_000_001,
			nil,
		},
		{
			"1d 1h 1min 1s 1ms after epoch start",
			"2000-01-02 01:01:01.000001",
			Time{
				Year:         2000,
				Month:        1,
				Day:          2,
				Hour:         1,
				Minutes:      1,
				Seconds:      1,
				Microseconds: 1,
			},
			microsecondsInDay + microsecondsInHour + microsecondsInMinute + 1_000_001,
			nil,
		},
		{
			"3 months after epoch start",
			"2000-04-01 00:00:00",
			Time{
				Year:         2000,
				Month:        4,
				Day:          1,
				Hour:         0,
				Minutes:      0,
				Seconds:      0,
				Microseconds: 0,
			},
			(31 + 29 + 31) * microsecondsInDay,
			nil,
		},
		{
			"Leap year february 29th valid timestamp",
			"2000-02-29 12:34:56.123456",
			Time{
				Year:         2000,
				Month:        2,
				Day:          29,
				Hour:         12,
				Minutes:      34,
				Seconds:      56,
				Microseconds: 123456,
			},
			(31+28)*microsecondsInDay + 12*microsecondsInHour + 34*microsecondsInMinute + 56*1_000_000 + 123456,
			nil,
		},
		{
			"One year after epoch start - 2000 is a leap year",
			"2001-01-01 00:00:00",
			Time{
				Year:         2001,
				Month:        1,
				Day:          1,
				Hour:         0,
				Minutes:      0,
				Seconds:      0,
				Microseconds: 0,
			},
			microsecondsInYear + microsecondsInDay,
			nil,
		},
		{
			"Valid timestamp - 2020 and 2024 are leap years",
			"2004-06-15 12:34:56.123456",
			Time{
				Year:         2004,
				Month:        6,
				Day:          15,
				Hour:         12,
				Minutes:      34,
				Seconds:      56,
				Microseconds: 123456,
			},
			// 165 includes Feb 29th of the leap year 2004
			4*microsecondsInYear + (31+29+31+30+31+14)*microsecondsInDay + 12*microsecondsInHour + 34*microsecondsInMinute + 56*1_000_000 + 123456 + 1*microsecondsInDay,
			nil,
		},
		{
			"2100 is not a leap year year",
			"2101-01-31 00:00:00",
			Time{
				Year:         2101,
				Month:        1,
				Day:          31,
				Hour:         0,
				Minutes:      0,
				Seconds:      0,
				Microseconds: 0,
			},
			101*microsecondsInYear + 30*microsecondsInDay + leapYearsBetween(2000, 2101)*microsecondsInDay,
			nil,
		},
		{
			"Large valid timestamp - 4195 between 200 inclusive and 17500 exclusive",
			"17500-06-15 12:34:56.123456",
			Time{
				Year:         17500,
				Month:        6,
				Day:          15,
				Hour:         12,
				Minutes:      34,
				Seconds:      56,
				Microseconds: 123456,
			},
			15500*microsecondsInYear + (31+28+31+30+31+14)*microsecondsInDay + 12*microsecondsInHour + 34*microsecondsInMinute + 56*1_000_000 + 123456 + leapYearsBetween(2000, 17500)*microsecondsInDay,
			nil,
		},
		{
			"latest valid timestamp",
			"294276-12-31 23:59:59.999999",
			Time{
				Year:         294276,
				Month:        12,
				Day:          31,
				Hour:         23,
				Minutes:      59,
				Seconds:      59,
				Microseconds: 999999,
			},
			292277*microsecondsInYear - 1 + leapYearsBetween(2000, 294277)*microsecondsInDay,
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := ParseTimestamp(tt.timestampStr)
			if tt.err != nil {
				require.Error(t, err)
				assert.Equal(t, tt.err.Error(), err.Error())
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.expectedTotal, actual.TotalMicroseconds())
			assert.Equal(t, tt.timestampStr, actual.String())

			assert.Equal(t, actual, FromMicroseconds(tt.expectedTotal))
		})
	}
}

func TestIsLeapYear(t *testing.T) {
	t.Parallel()

	tests := []struct {
		year     int
		expected bool
	}{
		{0, true}, // 1 BC is leap year
		{1600, true},
		{1700, false},
		{1800, false},
		{1900, false}, // 1900 is not a leap year
		{2000, true},
		{2004, true},
		{2001, false},
		{2020, true},
		{2021, false},
		{2100, false}, // 2100 is not a leap year
		{2200, false},
		{2300, false},
		{2400, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.year), func(t *testing.T) {
			actual := isLeapYear(tt.year)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestDaysInMonth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		year     int
		month    int
		expected int
	}{
		{2021, 1, 31},
		{2021, 2, 28},
		{2020, 2, 29},
		{2021, 3, 31},
		{2021, 4, 30},
		{2021, 5, 31},
		{2021, 6, 30},
		{2021, 7, 31},
		{2021, 8, 31},
		{2021, 9, 30},
		{2021, 10, 31},
		{2021, 11, 30},
		{2021, 12, 31},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d-%02d", tt.year, tt.month), func(t *testing.T) {
			actual := daysInMonth(isLeapYear(tt.year), tt.month)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func leapYearsBetween(startYear, endYear int32) int64 {
	var count int64
	for y := startYear; y < endYear; y++ {
		if isLeapYear(int(y)) {
			count += 1
		}
	}
	return count
}
