package logging

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	conf := DefaultConfig()

	require.Nil(t, conf.Sampling)
	require.Equal(t, "time", conf.EncoderConfig.TimeKey)
	require.Equal(t, "severity", conf.EncoderConfig.LevelKey)
}

func TestParseLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want zapcore.Level
	}{
		{name: "debug", in: " DEBUG ", want: zapcore.DebugLevel},
		{name: "info", in: "info", want: zapcore.InfoLevel},
		{name: "warn", in: "warn", want: zapcore.WarnLevel},
		{name: "error", in: "error", want: zapcore.ErrorLevel},
		{name: "dpanic", in: "dpanic", want: zapcore.DPanicLevel},
		{name: "panic", in: "panic", want: zapcore.PanicLevel},
		{name: "fatal", in: "fatal", want: zapcore.FatalLevel},
		{name: "numeric", in: "-1", want: zapcore.DebugLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseLevel(tt.in)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseLevel_Invalid(t *testing.T) {
	t.Parallel()

	_, err := ParseLevel("verbose")

	require.Error(t, err)
}
