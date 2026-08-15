package observability

import (
	"fmt"
	"strings"
)

// RenderPrometheus renders all metrics in the Prometheus text exposition
// format. It is a stable, sorted rendering suitable for a /metrics scrape.
func (m *Metrics) RenderPrometheus() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var sb strings.Builder

	writeSimple := func(name, help, typ string, val int64) {
		if help != "" {
			fmt.Fprintf(&sb, "# HELP %s %s\n", name, help)
		}
		if typ != "" {
			fmt.Fprintf(&sb, "# TYPE %s %s\n", name, typ)
		}
		fmt.Fprintf(&sb, "%s %d\n", name, val)
	}

	writeLabeled := func(name string, labels map[string]int64, help, typ string) {
		fmt.Fprintf(&sb, "# HELP %s %s\n", name, help)
		fmt.Fprintf(&sb, "# TYPE %s %s\n", name, typ)
		// Sort labels for stable output.
		keys := sortedKeys(labels)
		for _, k := range keys {
			fmt.Fprintf(&sb, "%s{%s=\"%s\"} %d\n", name, labelName(name), k, labels[k])
		}
		if len(keys) == 0 {
			fmt.Fprintf(&sb, "%s 0\n", name)
		}
	}

	writeLabeled("quotaraft_operations_total", m.opsTotal, "Total operations by type.", "counter")
	writeLabeled("quotaraft_rejections_total", m.rejections, "Total rejections by reason code.", "counter")
	writeSimple("quotaraft_busy_retries_total", "Total database busy retries.", "counter", m.busyRetries)
	writeSimple("quotaraft_recover_ok_total", "Successful recoveries.", "counter", m.recoverOK)
	writeSimple("quotaraft_recover_failed_total", "Failed recoveries.", "counter", m.recoverFailed)
	writeSimple("quotaraft_expired_total", "Reservations expired by maintenance.", "counter", m.expiredTotal)
	writeSimple("quotaraft_refill_total", "Token-bucket refills materialized.", "counter", m.refillTotal)
	writeSimple("quotaraft_cycle_reset_total", "Cycle resets materialized.", "counter", m.cycleResetTotal)
	writeSimple("quotaraft_balance_change_total", "Total balance changes.", "counter", m.balanceChange)

	return sb.String()
}

func labelName(metric string) string {
	switch metric {
	case "quotaraft_operations_total":
		return "op"
	case "quotaraft_rejections_total":
		return "reason"
	default:
		return "kind"
	}
}

func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}
	return keys
}
