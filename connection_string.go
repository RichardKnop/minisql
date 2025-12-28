package minisql

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
	"go.uber.org/zap"
)

// ConnectionConfig holds parsed connection string parameters
type ConnectionConfig struct {
	FilePath       string // Database file path
	JournalEnabled bool   // Enable/disable rollback journal (default: true)
	LogLevel       string // Log level: debug, info, warn, error (default: warn)
	MaxCachedPages int    // Maximum number of pages to cache (default: 1000, 0 = use default)
}

// DefaultConnectionConfig returns default configuration
func DefaultConnectionConfig(filePath string) *ConnectionConfig {
	return &ConnectionConfig{
		FilePath:       filePath,
		JournalEnabled: true,
		LogLevel:       "warn",
		MaxCachedPages: minisql.PageCacheSize,
	}
}

// ParseConnectionString parses a connection string with optional query parameters.
//
// Format: /path/to/database.db?param1=value1&param2=value2
//
// Supported parameters:
//   - journal=true|false  : Enable/disable rollback journal (default: true)
//   - log_level=debug|info|warn|error : Set logging level (default: warn)
//
// Examples:
//   - "./my.db"                          : Default settings
//   - "./my.db?journal=false"            : Disable journaling
//   - "./my.db?log_level=debug"          : Enable debug logging
//   - "./my.db?journal=true&log_level=info" : Both settings
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

	// Parse journal parameter
	if journalStr := queryParams.Get("journal"); journalStr != "" {
		journal, err := strconv.ParseBool(journalStr)
		if err != nil {
			return nil, fmt.Errorf("invalid journal parameter: must be 'true' or 'false', got %q", journalStr)
		}
		config.JournalEnabled = journal
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

	return config, nil
}

// GetZapLevel converts log level string to zap.Level
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
