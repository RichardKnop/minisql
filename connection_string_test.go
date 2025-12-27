package minisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConnectionString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		connStr     string
		wantConfig  *ConnectionConfig
		wantErr     bool
		errContains string
	}{
		{
			name:    "simple path",
			connStr: "./test.db",
			wantConfig: &ConnectionConfig{
				FilePath:       "./test.db",
				JournalEnabled: true,
				LogLevel:       "warn",
			},
			wantErr: false,
		},
		{
			name:    "disable journal",
			connStr: "./test.db?journal=false",
			wantConfig: &ConnectionConfig{
				FilePath:       "./test.db",
				JournalEnabled: false,
				LogLevel:       "warn",
			},
			wantErr: false,
		},
		{
			name:    "set log level",
			connStr: "./test.db?log_level=debug",
			wantConfig: &ConnectionConfig{
				FilePath:       "./test.db",
				JournalEnabled: true,
				LogLevel:       "debug",
			},
			wantErr: false,
		},
		{
			name:        "invalid journal value",
			connStr:     "./test.db?journal=maybe",
			wantErr:     true,
			errContains: "invalid journal parameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConnectionString(tt.connStr)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantConfig, config)
		})
	}
}
