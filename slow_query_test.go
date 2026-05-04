package minisql

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestConnLogSlowQuery(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zap.WarnLevel)
	conn := &Conn{
		logger:             zap.New(core),
		slowQueryThreshold: 50 * time.Millisecond,
	}

	conn.logSlowQuery("select * from users", 49*time.Millisecond, nil)
	assert.Equal(t, 0, logs.Len())

	conn.logSlowQuery("select * from users", 50*time.Millisecond, nil)
	assert.Equal(t, 1, logs.Len())

	entry := logs.All()[0]
	assert.Equal(t, "slow query", entry.Message)
	assert.Equal(t, "select * from users", entry.ContextMap()["query"])
	assert.Equal(t, 50*time.Millisecond, entry.ContextMap()["duration"])
	assert.Equal(t, 50*time.Millisecond, entry.ContextMap()["threshold"])
}

func TestConnLogSlowQueryIncludesError(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zap.WarnLevel)
	conn := &Conn{
		logger:             zap.New(core),
		slowQueryThreshold: time.Millisecond,
	}

	err := errors.New("boom")
	conn.logSlowQuery("select broken", 2*time.Millisecond, err)

	assert.Equal(t, 1, logs.Len())
	assert.Equal(t, "boom", logs.All()[0].ContextMap()["error"])
}

func TestConnLogSlowQueryDisabled(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zap.WarnLevel)
	conn := &Conn{
		logger: zap.New(core),
	}

	conn.logSlowQuery("select * from users", time.Hour, nil)
	assert.Equal(t, 0, logs.Len())
}
