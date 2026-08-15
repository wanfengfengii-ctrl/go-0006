package service

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/quotaraft/quotaraft/internal/clock"
	"github.com/quotaraft/quotaraft/internal/domain"
	"github.com/quotaraft/quotaraft/internal/store"
)

// TestReserveTriggeredRefillKeepsLedgerConsistent verifies that when a
// reservation is created after one or more refill periods have elapsed, the
// lazy refill performed inside Reserve writes both the advanced balance
// projection and a matching refill ledger entry. Without the matching entry
// the projection drifts ahead of the immutable ledger and Reconcile fails.
func TestReserveTriggeredRefillKeepsLedgerConsistent(t *testing.T) {
	svc, clk, _ := newTestService(t)
	// capacity 10, initial 5, refill 1 token per 1s.
	mustCreateTokenBucket(t, svc, "tnt", "tb", 10, 5, 1, 1000)

	// Advance past 3 refill intervals without running maintenance, so the
	// refill is materialized lazily by Reserve (not by a maintenance pass).
	clk.Set(3_000_000_000) // t=3s: 3 intervals elapsed -> +3 tokens (5 -> 8).

	resp := mustReserve(t, svc, ReserveRequest{
		TenantID: "tnt",
		Items:    []Item{{Resource: "tb", Amount: 2}},
	})

	// The lazy refill raised the balance from 5 to 8 before freezing 2.
	if len(resp.Balances) != 1 || resp.Balances[0].Resource != "tb" {
		t.Fatalf("expected one balance for tb, got %+v", resp.Balances)
	}
	if got := resp.Balances[0].Balance; got != 8 {
		t.Fatalf("balance = %d, want 8 (5 initial + 3 refilled)", got)
	}
	if got := resp.Balances[0].Frozen; got != 2 {
		t.Fatalf("frozen = %d, want 2", got)
	}

	// A refill ledger entry must exist for the lazy refill, in addition to
	// the opening refill entry recorded at policy creation.
	entries, _ := svc.ListLedger(context.Background(), "tnt", 0, 1000)
	refills := 0
	var lazyAmount int64
	for _, e := range entries {
		if e.Type == string(domain.EntryRefill) {
			refills++
			if e.Amount == 3 {
				lazyAmount = e.Amount
			}
		}
	}
	if refills != 2 { // opening refill (5) + lazy refill (3)
		t.Fatalf("refill entries = %d, want 2 (opening + lazy)", refills)
	}
	if lazyAmount != 3 {
		t.Fatalf("lazy refill amount = %d, want 3", lazyAmount)
	}

	// The online projection must equal a fresh replay of the ledger.
	rec, err := svc.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !rec.OK {
		t.Fatalf("reconcile not OK after reserve-triggered refill: %+v", rec.Mismatches)
	}
}

// TestReserveTriggeredRefillSurvivesRecovery verifies that after a
// reserve-triggered refill, a restart (Recover) does not fault the service and
// preserves the refilled balance. This catches the recovery-failure symptom:
// before the fix, Recover's reconcile detects the missing refill entry and
// faults the service into read-only.
func TestReserveTriggeredRefillSurvivesRecovery(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "quotaraft.db")

	clk := clock.NewManual(0)
	st, err := store.New(store.Options{Path: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	svc := New(st, clk, WithIDGenerator(NewCounterIDGenerator("t")))
	if err := svc.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustCreateTokenBucket(t, svc, "tnt", "tb", 10, 5, 1, 1000)

	// Advance 3 intervals and reserve, triggering a lazy refill (5 -> 8).
	clk.Set(3_000_000_000)
	mustReserve(t, svc, ReserveRequest{
		TenantID: "tnt",
		Items:    []Item{{Resource: "tb", Amount: 2}},
	})
	balBefore, _ := svc.GetBalance(context.Background(), "tnt", "tb")
	if balBefore.Balance != 8 || balBefore.Frozen != 2 {
		t.Fatalf("before restart: balance=%d frozen=%d, want 8/2", balBefore.Balance, balBefore.Frozen)
	}

	// Restart at the same logical time using the same DB file.
	_ = st.Close()
	st2, err := store.New(store.Options{Path: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	svc2 := New(st2, clk, WithIDGenerator(NewCounterIDGenerator("t")))
	if err := svc2.Recover(context.Background()); err != nil {
		t.Fatalf("recover after restart: %v", err)
	}
	if svc2.Faulted() {
		t.Fatalf("service faulted after restart: %s", svc2.FaultReason())
	}
	balAfter, _ := svc2.GetBalance(context.Background(), "tnt", "tb")
	if balAfter.Balance != 8 || balAfter.Frozen != 2 {
		t.Fatalf("after restart: balance=%d frozen=%d, want 8/2", balAfter.Balance, balAfter.Frozen)
	}

	// A second restart must add no further entries (recovery is idempotent).
	entriesBefore, _ := svc2.ListLedger(context.Background(), "tnt", 0, 1000)
	_ = st2.Close()
	st3, err := store.New(store.Options{Path: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st3.Close() })
	svc3 := New(st3, clk, WithIDGenerator(NewCounterIDGenerator("t")))
	if err := svc3.Recover(context.Background()); err != nil {
		t.Fatalf("recover2: %v", err)
	}
	entriesAfter, _ := svc3.ListLedger(context.Background(), "tnt", 0, 1000)
	if len(entriesAfter) != len(entriesBefore) {
		t.Fatalf("second restart added entries: %d vs %d", len(entriesAfter), len(entriesBefore))
	}
}

// TestReserveAtCapacityRefillConsistent verifies the at-capacity path: when
// whole intervals elapse but the bucket is already full (no tokens added),
// settle advances last_refill_time with no refill entry, and the projection
// stays consistent. This guards that the fix does not emit spurious entries.
func TestReserveAtCapacityRefillConsistent(t *testing.T) {
	svc, clk, _ := newTestService(t)
	// Full bucket: capacity 10, initial 10, refill 1/1s.
	mustCreateTokenBucket(t, svc, "tnt", "tb", 10, 10, 1, 1000)

	clk.Set(3_000_000_000) // 3 intervals elapsed but bucket already full.
	resp := mustReserve(t, svc, ReserveRequest{
		TenantID: "tnt",
		Items:    []Item{{Resource: "tb", Amount: 4}},
	})

	if got := resp.Balances[0].Balance; got != 10 {
		t.Fatalf("balance = %d, want 10 (no tokens added, already at capacity)", got)
	}
	if got := resp.Balances[0].Frozen; got != 4 {
		t.Fatalf("frozen = %d, want 4", got)
	}

	// Only the opening refill entry; the at-capacity settle adds none.
	entries, _ := svc.ListLedger(context.Background(), "tnt", 0, 1000)
	refills := 0
	for _, e := range entries {
		if e.Type == string(domain.EntryRefill) {
			refills++
		}
	}
	if refills != 1 {
		t.Fatalf("refill entries = %d, want 1 (opening only)", refills)
	}

	rec, err := svc.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !rec.OK {
		t.Fatalf("reconcile not OK at-capacity: %+v", rec.Mismatches)
	}
}

// TestReserveTriggeredRefillThenCommitConsistent verifies the full lifecycle:
// a reserve-triggered refill followed by a commit keeps the ledger and the
// projection consistent end to end.
func TestReserveTriggeredRefillThenCommitConsistent(t *testing.T) {
	svc, clk, _ := newTestService(t)
	mustCreateTokenBucket(t, svc, "tnt", "tb", 10, 5, 1, 1000)

	clk.Set(3_000_000_000) // +3 tokens -> 8.
	res := mustReserve(t, svc, ReserveRequest{
		TenantID: "tnt",
		Items:    []Item{{Resource: "tb", Amount: 6}},
	})
	if got := res.Balances[0].Balance; got != 8 {
		t.Fatalf("post-reserve balance = %d, want 8", got)
	}
	if got := res.Balances[0].Available; got != 2 {
		t.Fatalf("post-reserve available = %d, want 2", got)
	}

	// Commit charging 4 of the 6 frozen: balance 8 - 4 = 4, frozen 0.
	if _, err := svc.Commit(context.Background(), CommitRequest{
		ReservationID: res.ReservationID,
		TenantID:      "tnt",
		Usage:         map[string]int64{"tb": 4},
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	bal, _ := svc.GetBalance(context.Background(), "tnt", "tb")
	if bal.Balance != 4 {
		t.Fatalf("post-commit balance = %d, want 4", bal.Balance)
	}
	if bal.Frozen != 0 {
		t.Fatalf("post-commit frozen = %d, want 0", bal.Frozen)
	}

	rec, err := svc.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !rec.OK {
		t.Fatalf("reconcile not OK after commit: %+v", rec.Mismatches)
	}
}
