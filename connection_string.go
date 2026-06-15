package minisql

import (
	"encoding/hex"
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

// DefaultSortMemLimit is the default maximum bytes of row data held in memory
// before an ORDER BY query spills sorted runs to disk (4 MiB).
const DefaultSortMemLimit = 4 * 1024 * 1024

// DefaultHNSWVecCacheSize is the default number of vector entries cached per
// HNSW index.  Each slot holds the full float32 slice for one row (dims×4 bytes).
const DefaultHNSWVecCacheSize = minisql.DefaultHNSWVecCacheSize

// ConnectionConfig holds parsed connection string parameters.
type ConnectionConfig struct {
	FilePath               string          // Database file path
	WALCheckpointThreshold int             // Auto-checkpoint threshold in WAL frames (default: 1000)
	WALWriteBufferSize     int             // WAL write-buffer size in bytes (default: 64 KiB; 0 = flush every commit)
	LogLevel               string          // Log level: debug, info, warn, error (default: warn)
	MaxCachedPages         int             // Maximum number of pages to cache (default: 2000, 0 = use default)
	SlowQueryThreshold     time.Duration   // Log queries at WARN when elapsed time meets or exceeds this duration (0 = disabled)
	Synchronous            SynchronousMode // WAL fsync mode: off, normal (default), full
	ParallelScan           bool            // Enable concurrent leaf-page scanning (default: false)
	EncryptionKey          []byte          // AES-256-CTR page encryption key (nil = no encryption)
	SortMemLimit           int64           // Max bytes in memory before ORDER BY spills to disk (default: 4 MiB; 0 = disabled)
	HNSWVecCacheSize       int             // Max vector entries per HNSW index LRU cache (default: 4096)
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
		SortMemLimit:           DefaultSortMemLimit,
		HNSWVecCacheSize:       DefaultHNSWVecCacheSize,
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
//   - parallel_scan=on|off              : Enable concurrent leaf-page scanning (default: off)
//   - encryption_key=<hex>             : Hex-encoded AES-256-CTR encryption key (default: no encryption)
//   - hnsw_vec_cache_size=N            : Per-index HNSW vector LRU cache size in entries (default: 4096)
//
// Examples:
//   - "./my.db"                                       : Default settings
//   - "./my.db?log_level=debug"                       : Enable debug logging
//   - "./my.db?wal_checkpoint_threshold=500"          : Auto-checkpoint every 500 frames
//   - "./my.db?wal_write_buffer_size=0"               : Disable write batching (flush every commit)
//   - "./my.db?synchronous=full"                      : fsync on every commit (maximum durability)
//   - "./my.db?parallel_scan=on"                      : Enable parallel full table scans
//   - "./my.db?encryption_key=deadbeef..."            : Enable transparent page encryption
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

	// Parse wal_write_buffer_size parameter (0 = flush every commit; max 256 MiB)
	if bufSizeStr := queryParams.Get("wal_write_buffer_size"); bufSizeStr != "" {
		bufSize, err := strconv.Atoi(bufSizeStr)
		if err != nil || bufSize < 0 {
			return nil, fmt.Errorf("invalid wal_write_buffer_size parameter: must be a non-negative integer, got %q", bufSizeStr)
		}
		if bufSize > 256*1024*1024 {
			return nil, fmt.Errorf("invalid wal_write_buffer_size parameter: %d exceeds maximum of 256 MiB", bufSize)
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

	// Parse max_cached_pages parameter (0 = use default)
	if maxPagesStr := queryParams.Get("max_cached_pages"); maxPagesStr != "" {
		maxPages, err := strconv.Atoi(maxPagesStr)
		if err != nil || maxPages < 0 {
			return nil, fmt.Errorf("invalid max_cached_pages parameter: must be a non-negative integer (0 = use default), got %q", maxPagesStr)
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

	// Parse parallel_scan parameter
	if psStr := queryParams.Get("parallel_scan"); psStr != "" {
		switch strings.ToLower(psStr) {
		case "on", "1", "true":
			config.ParallelScan = true
		case "off", "0", "false":
			config.ParallelScan = false
		default:
			return nil, fmt.Errorf("invalid parallel_scan parameter: expected on or off, got %q", psStr)
		}
	}

	// Parse encryption_key parameter (hex-encoded, minimum 16 bytes / 32 hex chars)
	if keyHex := queryParams.Get("encryption_key"); keyHex != "" {
		key, err := hex.DecodeString(keyHex)
		if err != nil {
			return nil, fmt.Errorf("invalid encryption_key parameter: must be a hex-encoded string: %w", err)
		}
		if len(key) < 16 {
			return nil, fmt.Errorf("invalid encryption_key parameter: key too short (%d bytes), minimum is 16 bytes (32 hex chars)", len(key))
		}
		config.EncryptionKey = key
	}

	// Parse hnsw_vec_cache_size parameter (entries per index; must be > 0)
	if sizeStr := queryParams.Get("hnsw_vec_cache_size"); sizeStr != "" {
		size, err := strconv.Atoi(sizeStr)
		if err != nil || size <= 0 {
			return nil, fmt.Errorf("invalid hnsw_vec_cache_size parameter: must be a positive integer, got %q", sizeStr)
		}
		config.HNSWVecCacheSize = size
	}

	// Parse sort_mem_limit parameter (bytes; 0 = disable external sort; max 2 GiB)
	if limitStr := queryParams.Get("sort_mem_limit"); limitStr != "" {
		limit, err := strconv.ParseInt(limitStr, 10, 64)
		if err != nil || limit < 0 {
			return nil, fmt.Errorf("invalid sort_mem_limit parameter: must be a non-negative integer (bytes), got %q", limitStr)
		}
		if limit > 2*1024*1024*1024 {
			return nil, fmt.Errorf("invalid sort_mem_limit parameter: %d exceeds maximum of 2 GiB", limit)
		}
		config.SortMemLimit = limit
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
