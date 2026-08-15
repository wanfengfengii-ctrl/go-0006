// Package observability provides structured logging, Prometheus metrics, and
// health/readiness checks for QuotaRaft.
package observability

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Logger wraps slog with a component tag. Using slog keeps the dependency
// surface small (stdlib only) and produces structured, parseable output.
type Logger struct {
	*slog.Logger
}

// NewLogger creates a JSON structured logger writing to w. In production the
// level can be raised; tests use a discard sink.
func NewLogger(w io.Writer, level slog.Level) Logger {
	if w == nil {
		w = os.Stderr
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return Logger{slog.New(h).With("component", "quotaraft")}
}

// DiscardLogger returns a logger that drops all output, for tests.
func DiscardLogger() Logger {
	return NewLogger(io.Discard, slog.LevelError)
}

// Metrics holds Prometheus-style counters and a snapshot-able state. The
// implementation deliberately avoids a third-party metrics library so the
// project has no extra dependencies and so tests can assert on counters
// deterministically. The /metrics endpoint renders the counters in the
// Prometheus text format.
type Metrics struct {
	mu sync.Mutex

	opsTotal        map[string]int64 // op -> count
	rejections      map[string]int64 // reason -> count
	busyRetries     int64
	recoverOK       int64
	recoverFailed   int64
	expiredTotal    int64
	refillTotal     int64
	cycleResetTotal int64
	balanceChange   int64
}

// NewMetrics returns an empty Metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		opsTotal:   make(map[string]int64),
		rejections: make(map[string]int64),
	}
}

// IncOp increments the operation counter for op.
func (m *Metrics) IncOp(op string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.opsTotal[op]++
}

// IncRejection increments the rejection counter for a reason code.
func (m *Metrics) IncRejection(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rejections[reason]++
}

// IncBusyRetry increments the busy-retry counter.
func (m *Metrics) IncBusyRetry() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.busyRetries++
}

// IncRecover records a recovery outcome.
func (m *Metrics) IncRecover(ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ok {
		m.recoverOK++
	} else {
		m.recoverFailed++
	}
}

// IncMaintenance records maintenance pass counters.
func (m *Metrics) IncMaintenance(expired, refills, resets int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expiredTotal += int64(expired)
	m.refillTotal += int64(refills)
	m.cycleResetTotal += int64(resets)
}

// IncBalanceChange records that a balance changed.
func (m *Metrics) IncBalanceChange() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.balanceChange++
}

// Snapshot returns a JSON-serializable snapshot of all metrics, useful for the
// diagnostics endpoint and for tests.
func (m *Metrics) Snapshot() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]any{
		"operations":          copyMap(m.opsTotal),
		"rejections":          copyMap(m.rejections),
		"busy_retries":       m.busyRetries,
		"recover_ok":          m.recoverOK,
		"recover_failed":      m.recoverFailed,
		"expired_total":       m.expiredTotal,
		"refill_total":        m.refillTotal,
		"cycle_reset_total":   m.cycleResetTotal,
		"balance_change_total": m.balanceChange,
	}
	return out
}

func copyMap(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// MarshalJSONSnapshot renders the metrics snapshot as JSON.
func MarshalJSONSnapshot(m *Metrics) ([]byte, error) {
	return json.Marshal(m.Snapshot())
}

// HealthChecker reports readiness based on the service state.
type HealthChecker interface {
	Readyz(ctx context.Context) bool
	Healthz(ctx context.Context) bool
}

// Ping is a minimal health probe used until the server is wired.
type Ping struct{}

func (Ping) Readyz(context.Context) bool { return true }
func (Ping) Healthz(context.Context) bool { return true }

// Deadline returns a time.Time far in the future; kept to avoid importing time
// elsewhere in callers that need a deadline.
func Deadline(d time.Duration) time.Time { return time.Now().Add(d) }
