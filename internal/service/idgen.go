package service

import (
	"fmt"
	"sync/atomic"
	"time"
)

// defaultIDGenerator produces monotonically-numbered reservation IDs. It uses
// a process-wide counter combined with the wall clock so that IDs are unique
// and ordered; tests that need fully deterministic IDs inject a CounterIDGenerator.
type defaultIDGenerator struct {
	counter atomic.Int64
}

func (g *defaultIDGenerator) NewID() string {
	n := g.counter.Add(1)
	return fmt.Sprintf("res-%d-%d", n, time.Now().UTC().UnixNano())
}

// CounterIDGenerator is a deterministic ID generator for tests. Each call
// returns "res-<prefix>-<n>" with n incrementing from 1.
type CounterIDGenerator struct {
	prefix  string
	counter atomic.Int64
}

// NewCounterIDGenerator creates a deterministic generator with the given prefix.
func NewCounterIDGenerator(prefix string) *CounterIDGenerator {
	return &CounterIDGenerator{prefix: prefix}
}

// NewID returns the next deterministic ID.
func (g *CounterIDGenerator) NewID() string {
	n := g.counter.Add(1)
	return fmt.Sprintf("res-%s-%d", g.prefix, n)
}

// Ensure the generators satisfy IDGenerator.
var (
	_ IDGenerator = (*defaultIDGenerator)(nil)
	_ IDGenerator = (*CounterIDGenerator)(nil)
)

// realNow returns the current UTC time, kept as a var so it can be referenced
// from other files without re-importing time.
func realNow() time.Time { return time.Now().UTC() }

// timeSecondNS is one second in nanoseconds, used by the default maintenance
// interval.
const timeSecondNS = int64(time.Second)
