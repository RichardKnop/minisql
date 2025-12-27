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
				MaxCachedPages: 1000,
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
				MaxCachedPages: 1000,
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
				MaxCachedPages: 1000,
			},
			wantErr: false,
		},
		{
			name:    "set max cached pages",
			connStr: "./test.db?max_cached_pages=500",
			wantConfig: &ConnectionConfig{
				FilePath:       "./test.db",
				JournalEnabled: true,
				LogLevel:       "warn",
				MaxCachedPages: 500,
			},
			wantErr: false,
		},
		{
			name:    "all parameters",
			connStr: "./test.db?journal=false&log_level=info&max_cached_pages=2000",
			wantConfig: &ConnectionConfig{
				FilePath:       "./test.db",
				JournalEnabled: false,
				LogLevel:       "info",
				MaxCachedPages: 2000,
			},
			wantErr: false,
		},
		{
			name:        "invalid max_cached_pages - negative",
			connStr:     "./test.db?max_cached_pages=-100",
			wantErr:     true,
			errContains: "must be non-negative",
		},
		{
			name:        "invalid max_cached_pages - not a number",
			connStr:     "./test.db?max_cached_pages=abc",
			wantErr:     true,
			errContains: "must be a positive integer",
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
