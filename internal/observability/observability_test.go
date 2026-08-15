package observability

import (
	"strings"
	"testing"
)

func TestMetricsRender(t *testing.T) {
	m := NewMetrics()
	m.IncOp("deduct")
	m.IncOp("deduct")
	m.IncRejection("INSUFFICIENT_QUOTA")
	m.IncBusyRetry()
	m.IncMaintenance(1, 2, 3)

	out := m.RenderPrometheus()
	if !strings.Contains(out, "quotaraft_operations_total") {
		t.Fatalf("missing operations metric:\n%s", out)
	}
	if !strings.Contains(out, "op=\"deduct\"") {
		t.Fatalf("missing deduct label:\n%s", out)
	}
	if !strings.Contains(out, "reason=\"INSUFFICIENT_QUOTA\"") {
		t.Fatalf("missing rejection label:\n%s", out)
	}
	if !strings.Contains(out, "quotaraft_busy_retries_total 1") {
		t.Fatalf("missing busy retries:\n%s", out)
	}
	if !strings.Contains(out, "quotaraft_refill_total 2") {
		t.Fatalf("missing refill total:\n%s", out)
	}
	if !strings.Contains(out, "quotaraft_cycle_reset_total 3") {
		t.Fatalf("missing cycle reset total:\n%s", out)
	}
}

func TestSnapshot(t *testing.T) {
	m := NewMetrics()
	m.IncOp("reserve")
	snap := m.Snapshot()
	if snap["operations"].(map[string]int64)["reserve"] != 1 {
		t.Fatalf("snapshot mismatch: %+v", snap)
	}
}

func TestLoggerDiscard(t *testing.T) {
	l := DiscardLogger()
	l.Info("hello")
}
