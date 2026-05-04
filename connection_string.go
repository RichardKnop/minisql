package minisql

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/RichardKnop/minisql/internal/minisql"
	"go.uber.org/zap"
)

// SynchronousMode re-exports the internal type so callers can use it without
// importing the internal package.
type SynchronousMode = minisql.SynchronousMode

// Synchronous mode constants.
const (
	SynchronousOff    = minisql.SynchronousOff
	SynchronousNormal = minisql.SynchronousNormal
	SynchronousFull   = minisql.SynchronousFull
)

// DefaultWALCheckpointThreshold is the number of WAL frames that triggers an
// automatic checkpoint when WAL mode is enabled.
const DefaultWALCheckpointThreshold = 1000

// DefaultWALWriteBufferSize is the default write-buffer size for WAL frame
// batching (64 KiB ≈ 16 full-page frames).  A larger buffer reduces WriteAt
// syscall overhead for high-frequency single-row-per-transaction workloads at
// the cost of slightly higher data-loss exposure on unclean shutdown.
const DefaultWALWriteBufferSize = 64 * 1024

// ConnectionConfig holds parsed connection string parameters.
type ConnectionConfig struct {
	FilePath               string          // Database file path
	WALCheckpointThreshold int             // Auto-checkpoint threshold in WAL frames (default: 1000)
	WALWriteBufferSize     int             // WAL write-buffer size in bytes (default: 64 KiB; 0 = flush every commit)
	LogLevel               string          // Log level: debug, info, warn, error (default: warn)
	MaxCachedPages         int             // Maximum number of pages to cache (default: 2000, 0 = use default)
	SlowQueryThreshold     time.Duration   // Log queries at WARN when elapsed time meets or exceeds this duration (0 = disabled)
	Synchronous            SynchronousMode // WAL fsync mode: off, normal (default), full
}

// DefaultConnectionConfig returns default configuration.
func DefaultConnectionConfig(filePath string) *ConnectionConfig {
	return &ConnectionConfig{
		FilePath:               filePath,
		WALCheckpointThreshold: DefaultWALCheckpointThreshold,
		WALWriteBufferSize:     DefaultWALWriteBufferSize,
		LogLevel:               "warn",
		MaxCachedPages:         minisql.PageCacheSize,
		Synchronous:            SynchronousNormal,
	}
}

// ParseConnectionString parses a connection string with optional query parameters.
//
// Format: /path/to/database.db?param1=value1&param2=value2
//
// Supported parameters:
//   - wal_checkpoint_threshold=N        : Auto-checkpoint after N WAL frames (default: 1000; 0 = disabled)
//   - wal_write_buffer_size=N           : WAL write-buffer in bytes (default: 65536; 0 = flush every commit)
//   - log_level=debug|info|warn|error   : Set logging level (default: warn)
//   - max_cached_pages=N                : Page cache size in pages (default: 2000)
//   - slow_query_threshold=50ms         : Log queries taking at least this long (0 = disabled)
//   - synchronous=off|normal|full       : WAL fsync mode (default: normal, matching SQLite WAL default)
//
// Examples:
//   - "./my.db"                                       : Default settings
//   - "./my.db?log_level=debug"                       : Enable debug logging
//   - "./my.db?wal_checkpoint_threshold=500"          : Auto-checkpoint every 500 frames
//   - "./my.db?wal_write_buffer_size=0"               : Disable write batching (flush every commit)
//   - "./my.db?synchronous=full"                      : fsync on every commit (maximum durability)
//   - "./my.db?log_level=info&max_cached_pages=500"   : Multiple parameters
func ParseConnectionString(connStr string) (*ConnectionConfig, error) {
	// Split on first '?' to separate path from query params
	parts := strings.SplitN(connStr, "?", 2)

	config := DefaultConnectionConfig(parts[0])

	// No query parameters
	if len(parts) == 1 {
		return config, nil
	}

	// Parse query parameters
	queryParams, err := url.ParseQuery(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid connection string query parameters: %w", err)
	}

	// Parse wal_checkpoint_threshold parameter
	if threshStr := queryParams.Get("wal_checkpoint_threshold"); threshStr != "" {
		thresh, err := strconv.Atoi(threshStr)
		if err != nil || thresh < 0 {
			return nil, fmt.Errorf("invalid wal_checkpoint_threshold parameter: must be a non-negative integer, got %q", threshStr)
		}
		config.WALCheckpointThreshold = thresh
	}

	// Parse wal_write_buffer_size parameter
	if bufSizeStr := queryParams.Get("wal_write_buffer_size"); bufSizeStr != "" {
		bufSize, err := strconv.Atoi(bufSizeStr)
		if err != nil || bufSize < 0 {
			return nil, fmt.Errorf("invalid wal_write_buffer_size parameter: must be a non-negative integer, got %q", bufSizeStr)
		}
		config.WALWriteBufferSize = bufSize
	}

	// Parse log_level parameter
	if logLevel := queryParams.Get("log_level"); logLevel != "" {
		logLevel = strings.ToLower(logLevel)
		switch logLevel {
		case "debug", "info", "warn", "error":
			config.LogLevel = logLevel
		default:
			return nil, fmt.Errorf("invalid log_level parameter: must be 'debug', 'info', 'warn', or 'error', got %q", logLevel)
		}
	}

	// Parse max_cached_pages parameter
	if maxPagesStr := queryParams.Get("max_cached_pages"); maxPagesStr != "" {
		maxPages, err := strconv.Atoi(maxPagesStr)
		if err != nil {
			return nil, fmt.Errorf("invalid max_cached_pages parameter: must be a positive integer, got %q", maxPagesStr)
		}
		if maxPages < 0 {
			return nil, fmt.Errorf("invalid max_cached_pages parameter: must be non-negative, got %d", maxPages)
		}
		config.MaxCachedPages = maxPages
	}

	// Parse slow_query_threshold parameter
	if thresholdStr := queryParams.Get("slow_query_threshold"); thresholdStr != "" {
		threshold, err := time.ParseDuration(thresholdStr)
		if err != nil || threshold < 0 {
			return nil, fmt.Errorf("invalid slow_query_threshold parameter: must be a non-negative duration, got %q", thresholdStr)
		}
		config.SlowQueryThreshold = threshold
	}

	// Parse synchronous parameter
	if syncStr := queryParams.Get("synchronous"); syncStr != "" {
		switch strings.ToLower(syncStr) {
		case "off", "0":
			config.Synchronous = SynchronousOff
		case "normal", "1":
			config.Synchronous = SynchronousNormal
		case "full", "2":
			config.Synchronous = SynchronousFull
		default:
			return nil, fmt.Errorf("invalid synchronous parameter: expected off, normal, or full, got %q", syncStr)
		}
	}

	return config, nil
}

// GetZapLevel converts log level string to zap.Level.
func (c *ConnectionConfig) GetZapLevel() zap.AtomicLevel {
	switch c.LogLevel {
	case "debug":
		return zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		return zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		return zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		return zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		return zap.NewAtomicLevelAt(zap.WarnLevel)
	}
}
