package minisql

import (
	"testing"
	"time"

	"github.com/RichardKnop/minisql/internal/minisql"
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
				FilePath:               "./test.db",
				WALCheckpointThreshold: DefaultWALCheckpointThreshold,
				WALWriteBufferSize:     DefaultWALWriteBufferSize,
				LogLevel:               "warn",
				MaxCachedPages:         minisql.PageCacheSize,
				Synchronous:            SynchronousNormal,
			},
			wantErr: false,
		},
		{
			name:    "set log level",
			connStr: "./test.db?log_level=debug",
			wantConfig: &ConnectionConfig{
				FilePath:               "./test.db",
				WALCheckpointThreshold: DefaultWALCheckpointThreshold,
				WALWriteBufferSize:     DefaultWALWriteBufferSize,
				LogLevel:               "debug",
				MaxCachedPages:         minisql.PageCacheSize,
				Synchronous:            SynchronousNormal,
			},
			wantErr: false,
		},
		{
			name:    "set max cached pages",
			connStr: "./test.db?max_cached_pages=500",
			wantConfig: &ConnectionConfig{
				FilePath:               "./test.db",
				WALCheckpointThreshold: DefaultWALCheckpointThreshold,
				WALWriteBufferSize:     DefaultWALWriteBufferSize,
				LogLevel:               "warn",
				MaxCachedPages:         500,
				Synchronous:            SynchronousNormal,
			},
			wantErr: false,
		},
		{
			name:    "custom checkpoint threshold",
			connStr: "./test.db?wal_checkpoint_threshold=500",
			wantConfig: &ConnectionConfig{
				FilePath:               "./test.db",
				WALCheckpointThreshold: 500,
				WALWriteBufferSize:     DefaultWALWriteBufferSize,
				LogLevel:               "warn",
				MaxCachedPages:         minisql.PageCacheSize,
				Synchronous:            SynchronousNormal,
			},
			wantErr: false,
		},
		{
			name:    "custom write buffer size",
			connStr: "./test.db?wal_write_buffer_size=131072",
			wantConfig: &ConnectionConfig{
				FilePath:               "./test.db",
				WALCheckpointThreshold: DefaultWALCheckpointThreshold,
				WALWriteBufferSize:     131072,
				LogLevel:               "warn",
				MaxCachedPages:         minisql.PageCacheSize,
				Synchronous:            SynchronousNormal,
			},
			wantErr: false,
		},
		{
			name:    "zero write buffer size disables batching",
			connStr: "./test.db?wal_write_buffer_size=0",
			wantConfig: &ConnectionConfig{
				FilePath:               "./test.db",
				WALCheckpointThreshold: DefaultWALCheckpointThreshold,
				WALWriteBufferSize:     0,
				LogLevel:               "warn",
				MaxCachedPages:         minisql.PageCacheSize,
				Synchronous:            SynchronousNormal,
			},
			wantErr: false,
		},
		{
			name:    "all parameters",
			connStr: "./test.db?wal_checkpoint_threshold=200&log_level=info&max_cached_pages=4000&slow_query_threshold=75ms",
			wantConfig: &ConnectionConfig{
				FilePath:               "./test.db",
				WALCheckpointThreshold: 200,
				WALWriteBufferSize:     DefaultWALWriteBufferSize,
				LogLevel:               "info",
				MaxCachedPages:         4000,
				SlowQueryThreshold:     75 * time.Millisecond,
				Synchronous:            SynchronousNormal,
			},
			wantErr: false,
		},
		{
			name:    "zero checkpoint threshold disables auto-checkpoint",
			connStr: "./test.db?wal_checkpoint_threshold=0",
			wantConfig: &ConnectionConfig{
				FilePath:               "./test.db",
				WALCheckpointThreshold: 0,
				WALWriteBufferSize:     DefaultWALWriteBufferSize,
				LogLevel:               "warn",
				MaxCachedPages:         minisql.PageCacheSize,
				Synchronous:            SynchronousNormal,
			},
			wantErr: false,
		},
		{
			name:    "synchronous=full",
			connStr: "./test.db?synchronous=full",
			wantConfig: &ConnectionConfig{
				FilePath:               "./test.db",
				WALCheckpointThreshold: DefaultWALCheckpointThreshold,
				WALWriteBufferSize:     DefaultWALWriteBufferSize,
				LogLevel:               "warn",
				MaxCachedPages:         minisql.PageCacheSize,
				Synchronous:            SynchronousFull,
			},
			wantErr: false,
		},
		{
			name:    "synchronous=normal explicit",
			connStr: "./test.db?synchronous=normal",
			wantConfig: &ConnectionConfig{
				FilePath:               "./test.db",
				WALCheckpointThreshold: DefaultWALCheckpointThreshold,
				WALWriteBufferSize:     DefaultWALWriteBufferSize,
				LogLevel:               "warn",
				MaxCachedPages:         minisql.PageCacheSize,
				Synchronous:            SynchronousNormal,
			},
			wantErr: false,
		},
		{
			name:    "synchronous=off",
			connStr: "./test.db?synchronous=off",
			wantConfig: &ConnectionConfig{
				FilePath:               "./test.db",
				WALCheckpointThreshold: DefaultWALCheckpointThreshold,
				WALWriteBufferSize:     DefaultWALWriteBufferSize,
				LogLevel:               "warn",
				MaxCachedPages:         minisql.PageCacheSize,
				Synchronous:            SynchronousOff,
			},
			wantErr: false,
		},
		{
			name:        "invalid wal_checkpoint_threshold - negative",
			connStr:     "./test.db?wal_checkpoint_threshold=-1",
			wantErr:     true,
			errContains: "invalid wal_checkpoint_threshold",
		},
		{
			name:        "invalid wal_checkpoint_threshold - not a number",
			connStr:     "./test.db?wal_checkpoint_threshold=abc",
			wantErr:     true,
			errContains: "invalid wal_checkpoint_threshold",
		},
		{
			name:        "invalid wal_write_buffer_size - negative",
			connStr:     "./test.db?wal_write_buffer_size=-1",
			wantErr:     true,
			errContains: "invalid wal_write_buffer_size",
		},
		{
			name:        "invalid wal_write_buffer_size - not a number",
			connStr:     "./test.db?wal_write_buffer_size=abc",
			wantErr:     true,
			errContains: "invalid wal_write_buffer_size",
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
			name:        "invalid log level",
			connStr:     "./test.db?log_level=verbose",
			wantErr:     true,
			errContains: "invalid log_level parameter",
		},
		{
			name:        "invalid slow query threshold",
			connStr:     "./test.db?slow_query_threshold=soon",
			wantErr:     true,
			errContains: "invalid slow_query_threshold parameter",
		},
		{
			name:        "negative slow query threshold",
			connStr:     "./test.db?slow_query_threshold=-1ms",
			wantErr:     true,
			errContains: "invalid slow_query_threshold parameter",
		},
		{
			name:        "invalid synchronous value",
			connStr:     "./test.db?synchronous=extra",
			wantErr:     true,
			errContains: "invalid synchronous parameter",
		},
		{
			name:    "parallel_scan=on",
			connStr: "./test.db?parallel_scan=on",
			wantConfig: &ConnectionConfig{
				FilePath:               "./test.db",
				WALCheckpointThreshold: DefaultWALCheckpointThreshold,
				WALWriteBufferSize:     DefaultWALWriteBufferSize,
				LogLevel:               "warn",
				MaxCachedPages:         minisql.PageCacheSize,
				Synchronous:            SynchronousNormal,
				ParallelScan:           true,
			},
			wantErr: false,
		},
		{
			name:    "parallel_scan=off explicit",
			connStr: "./test.db?parallel_scan=off",
			wantConfig: &ConnectionConfig{
				FilePath:               "./test.db",
				WALCheckpointThreshold: DefaultWALCheckpointThreshold,
				WALWriteBufferSize:     DefaultWALWriteBufferSize,
				LogLevel:               "warn",
				MaxCachedPages:         minisql.PageCacheSize,
				Synchronous:            SynchronousNormal,
				ParallelScan:           false,
			},
			wantErr: false,
		},
		{
			name:        "invalid parallel_scan value",
			connStr:     "./test.db?parallel_scan=maybe",
			wantErr:     true,
			errContains: "invalid parallel_scan parameter",
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
