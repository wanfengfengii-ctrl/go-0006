package service

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/quotaraft/quotaraft/internal/clock"
	"github.com/quotaraft/quotaraft/internal/domain"
	"github.com/quotaraft/quotaraft/internal/store"
)

// newTestService creates a Service backed by an in-memory SQLite store and a
// manual clock, recovered and ready to accept traffic.
func newTestService(t *testing.T) (*Service, *clock.Manual, *store.SQLiteStore) {
	t.Helper()
	return newTestServiceAt(t, ":memory:", clock.NewManual(0))
}

func newTestServiceAt(t *testing.T, path string, clk *clock.Manual) (*Service, *clock.Manual, *store.SQLiteStore) {
	t.Helper()
	st, err := store.New(store.Options{Path: path})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := New(st, clk, WithIDGenerator(NewCounterIDGenerator("t")))
	if err := svc.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	return svc, clk, st
}

func mustCreateTenant(t *testing.T, svc *Service, id string) {
	t.Helper()
	if err := svc.CreateTenant(context.Background(), id); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
}

func mustCreateCyclePolicy(t *testing.T, svc *Service, tenant, resource string, limit int64) {
	t.Helper()
	mustCreateTenant(t, svc, tenant)
	_, err := svc.CreatePolicy(context.Background(), tenant, resource, PolicySpec{
		Type:          string(domain.PolicyCycle),
		CycleLimit:    limit,
		CycleLengthMS: 60_000, // 1 minute
	})
	if err != nil {
		t.Fatalf("create cycle policy: %v", err)
	}
}

func mustCreateTokenBucket(t *testing.T, svc *Service, tenant, resource string, capacity, initial, refill int64, refillMS int64) {
	t.Helper()
	mustCreateTenant(t, svc, tenant)
	_, err := svc.CreatePolicy(context.Background(), tenant, resource, PolicySpec{
		Type:             string(domain.PolicyTokenBucket),
		Capacity:         capacity,
		InitialTokens:    initial,
		RefillAmount:     refill,
		RefillIntervalMS: refillMS,
	})
	if err != nil {
		t.Fatalf("create token bucket: %v", err)
	}
}

// Acceptance #1: 25 concurrent unit deductions against a capacity-10 cycle
// quota: exactly 10 succeed, 15 insufficient, final balance 0, no negative
// balances or duplicate sequence numbers.
func TestAcceptance1_ConcurrentDeductionExactSuccess(t *testing.T) {
	svc, clk, _ := newTestService(t)
	clk.Set(1_000_000_000) // t=1s
	mustCreateCyclePolicy(t, svc, "tnt", "cpu", 10)

	var wg sync.WaitGroup
	var mu sync.Mutex
	succ, insuf := 0, 0
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.Deduct(context.Background(), DeductRequest{
				TenantID: "tnt",
				Items:    []Item{{Resource: "cpu", Amount: 1}},
			})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				succ++
			} else if AsCode(err) == CodeInsufficientQuota {
				insuf++
			} else {
				t.Errorf("deduction %d: unexpected error %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if succ != 10 || insuf != 15 {
		t.Fatalf("succ=%d insuf=%d, want 10/15", succ, insuf)
	}

	snap, err := svc.GetBalance(context.Background(), "tnt", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Balance != 0 {
		t.Fatalf("balance = %d, want 0", snap.Balance)
	}
	if snap.Frozen != 0 {
		t.Fatalf("frozen = %d, want 0", snap.Frozen)
	}

	// No duplicate sequence numbers and no negative balances.
	entries, err := svc.ListLedger(context.Background(), "tnt", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[int64]bool)
	for _, e := range entries {
		if seen[e.Seq] {
			t.Fatalf("duplicate seq %d", e.Seq)
		}
		seen[e.Seq] = true
		if e.Type == string(domain.EntryConsume) && e.Amount < 0 {
			t.Fatalf("negative amount in entry %+v", e)
		}
	}
}

func mustReserve(t *testing.T, svc *Service, req ReserveRequest) *ReserveResponse {
	t.Helper()
	resp, err := svc.Reserve(context.Background(), req)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	return resp
}

// Acceptance #2: atomic reservation across two resources; if one is
// insufficient the whole request fails and leaves everything unchanged.
func TestAcceptance2_AtomicReservationAllOrNothing(t *testing.T) {
	svc, clk, _ := newTestService(t)
	clk.Set(1_000_000_000)
	mustCreateCyclePolicy(t, svc, "tnt", "a", 10)
	mustCreateCyclePolicy(t, svc, "tnt", "b", 2)

	// Both sufficient: one pending reservation and freeze entries.
	resp := mustReserve(t, svc, ReserveRequest{
		TenantID: "tnt",
		Items:    []Item{{Resource: "a", Amount: 5}, {Resource: "b", Amount: 2}},
	})
	if resp.Status != string(domain.StatusPending) {
		t.Fatalf("status = %s, want pending", resp.Status)
	}
	for _, bal := range resp.Balances {
		switch bal.Resource {
		case "a":
			if bal.Frozen != 5 || bal.Available != 5 {
				t.Fatalf("a frozen=%d avail=%d, want 5/5", bal.Frozen, bal.Available)
			}
		case "b":
			if bal.Frozen != 2 || bal.Available != 0 {
				t.Fatalf("b frozen=%d avail=%d, want 2/0", bal.Frozen, bal.Available)
			}
		}
	}

	// Now reserve across a (has 5 available) and b (0 available): must fail.
	_, err := svc.Reserve(context.Background(), ReserveRequest{
		TenantID: "tnt",
		Items:    []Item{{Resource: "a", Amount: 1}, {Resource: "b", Amount: 1}},
	})
	if AsCode(err) != CodeInsufficientQuota {
		t.Fatalf("expected insufficient, got %v", err)
	}

	// Balances, reservations, and ledger unchanged for the failed attempt:
	// 'a' still has 5 frozen, no second reservation created.
	ba, _ := svc.GetBalance(context.Background(), "tnt", "a")
	bb, _ := svc.GetBalance(context.Background(), "tnt", "b")
	if ba.Frozen != 5 {
		t.Fatalf("a frozen = %d, want 5 (unchanged)", ba.Frozen)
	}
	if bb.Frozen != 2 {
		t.Fatalf("b frozen = %d, want 2 (unchanged)", bb.Frozen)
	}
	r, _ := svc.GetReservation(context.Background(), "tnt", resp.ReservationID)
	if r.Status != domain.StatusPending {
		t.Fatalf("reservation status changed: %s", r.Status)
	}
}

// Acceptance #3: commit returns the unused difference; repeat commit returns
// the first response; same key with changed usage returns idempotency
// conflict and leaves balance/ledger unchanged.
func TestAcceptance3_CommitIdempotencyAndConflict(t *testing.T) {
	svc, clk, _ := newTestService(t)
	clk.Set(1_000_000_000)
	mustCreateCyclePolicy(t, svc, "tnt", "r", 100)

	res := mustReserve(t, svc, ReserveRequest{
		TenantID: "tnt",
		Items:    []Item{{Resource: "r", Amount: 40}},
	})

	// Commit with usage 25: 15 returned.
	commit1, err := svc.Commit(context.Background(), CommitRequest{
		ReservationID:  res.ReservationID,
		TenantID:       "tnt",
		IdempotencyKey: "k1",
		Usage:          map[string]int64{"r": 25},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if commit1.Idempotent {
		t.Fatal("first commit should not be marked idempotent")
	}
	bal, _ := svc.GetBalance(context.Background(), "tnt", "r")
	// 100 - 25 charged = 75
	if bal.Balance != 75 {
		t.Fatalf("balance = %d, want 75", bal.Balance)
	}
	if bal.Frozen != 0 {
		t.Fatalf("frozen = %d, want 0", bal.Frozen)
	}

	// Repeat with same key + same usage: returns first response, idempotent.
	commit2, err := svc.Commit(context.Background(), CommitRequest{
		ReservationID:  res.ReservationID,
		TenantID:       "tnt",
		IdempotencyKey: "k1",
		Usage:          map[string]int64{"r": 25},
	})
	if err != nil {
		t.Fatalf("repeat commit: %v", err)
	}
	if !commit2.Idempotent {
		t.Fatal("repeat commit should be idempotent")
	}
	// Balance unchanged after replay.
	bal, _ = svc.GetBalance(context.Background(), "tnt", "r")
	if bal.Balance != 75 {
		t.Fatalf("balance after replay = %d, want 75", bal.Balance)
	}

	// Same key but different usage: conflict, nothing changes.
	_, err = svc.Commit(context.Background(), CommitRequest{
		ReservationID:  res.ReservationID,
		TenantID:       "tnt",
		IdempotencyKey: "k1",
		Usage:          map[string]int64{"r": 30},
	})
	if AsCode(err) != CodeIdempotencyConflict {
		t.Fatalf("expected conflict, got %v", err)
	}
	bal, _ = svc.GetBalance(context.Background(), "tnt", "r")
	if bal.Balance != 75 {
		t.Fatalf("balance after conflict = %d, want 75", bal.Balance)
	}
}

// Acceptance #4: commit, rollback and expiry racing on one pending reservation
// — exactly one terminal transition wins, refund/charge happens at most once.
func TestAcceptance4_TerminalRaceSingleWinner(t *testing.T) {
	for round := 0; round < 20; round++ {
		svc, clk, _ := newTestService(t)
		clk.Set(1_000_000_000)
		mustCreateCyclePolicy(t, svc, "tnt", "r", 100)

		res := mustReserve(t, svc, ReserveRequest{
			TenantID:  "tnt",
			Items:     []Item{{Resource: "r", Amount: 40}},
			ExpiresAt: int64(clk.Now() + 1_000_000_000), // expires in 1s
		})
		// Advance the clock so expiry is also eligible.
		clk.AdvanceDuration(2_000_000_000)

		var wg sync.WaitGroup
		// commit, rollback, and expiry maintenance all compete.
		wg.Add(3)
		go func() {
			defer wg.Done()
			_, err := svc.Commit(context.Background(), CommitRequest{ReservationID: res.ReservationID, TenantID: "tnt", Usage: map[string]int64{"r": 10}})
			// Either succeeds or is rejected as terminated; both are valid.
			if err != nil && AsCode(err) != CodeReservationTerminated {
				t.Errorf("commit unexpected: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			_, err := svc.Rollback(context.Background(), RollbackRequest{ReservationID: res.ReservationID, TenantID: "tnt"})
			if err != nil && AsCode(err) != CodeReservationTerminated {
				t.Errorf("rollback unexpected: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := svc.RunMaintenance(context.Background()); err != nil {
				t.Errorf("maintenance: %v", err)
			}
		}()
		wg.Wait()

		// Exactly one terminal transition must have won. Verify by inspecting
		// the final reservation status and the terminal entries for it: a
		// commit yields (commit_charge + release_unused), a rollback yields a
		// single rollback_release, and an expiry yields a single expiry_release.
		rFinal, _ := svc.GetReservation(context.Background(), "tnt", res.ReservationID)
		if !rFinal.Status.IsTerminal() {
			t.Fatalf("round %d: status = %s, want terminal", round, rFinal.Status)
		}
		entries, _ := svc.ListLedger(context.Background(), "tnt", 0, 10000)
		var charges, releases, rollbacks, expiries int
		for _, e := range entries {
			if e.ReservationID != res.ReservationID {
				continue
			}
			switch e.Type {
			case string(domain.EntryCommitCharge):
				charges++
			case string(domain.EntryReleaseUnused):
				releases++
			case string(domain.EntryRollbackRelease):
				rollbacks++
			case string(domain.EntryExpiryRelease):
				expiries++
			}
		}
		committed := charges == 1 && releases == 1 && rollbacks == 0 && expiries == 0
		rolledBack := charges == 0 && releases == 0 && rollbacks == 1 && expiries == 0
		expired := charges == 0 && releases == 0 && rollbacks == 0 && expiries == 1
		if !(committed || rolledBack || expired) {
			t.Fatalf("round %d: terminal entries not exactly one transition: charges=%d releases=%d rollbacks=%d expiries=%d status=%s",
				round, charges, releases, rollbacks, expiries, rFinal.Status)
		}

		// Reconciliation must pass: no double refund/charge.
		rec, err := svc.Reconcile(context.Background())
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if !rec.OK {
			t.Fatalf("round %d: reconcile not OK: %+v", round, rec.Mismatches)
		}
		// The frozen amount must be fully released regardless of winner.
		bal, _ := svc.GetBalance(context.Background(), "tnt", "r")
		if bal.Frozen != 0 {
			t.Fatalf("round %d: frozen = %d, want 0", round, bal.Frozen)
		}
		// Run maintenance again: no new entries (idempotent).
		m2, err := svc.RunMaintenance(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(m2.Entries) != 0 {
			t.Fatalf("round %d: re-run maintenance produced %d entries, want 0", round, len(m2.Entries))
		}
	}
}

// Acceptance #5: token-bucket boundaries with a manual clock. No increase
// before the interval; exact integer increase after full intervals; capped at
// capacity; identical balance and entries across a restart at the same time.
func TestAcceptance5_TokenBucketBoundariesAndRestart(t *testing.T) {
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
	mustCreateTokenBucket(t, svc, "tnt", "tb", 10, 5, 1, 1000) // 1 token per second

	// Before the interval: no increase.
	clk.Set(500_000_000) // 0.5s
	if _, err := svc.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
	bal, _ := svc.GetBalance(context.Background(), "tnt", "tb")
	if bal.Balance != 5 {
		t.Fatalf("before interval balance = %d, want 5", bal.Balance)
	}

	// After 3 full intervals: +3 (but capped at 10). 5+3=8.
	clk.Set(3_000_000_000)
	if _, err := svc.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
	bal, _ = svc.GetBalance(context.Background(), "tnt", "tb")
	if bal.Balance != 8 {
		t.Fatalf("after 3 intervals balance = %d, want 8", bal.Balance)
	}

	// Far past capacity: capped at 10.
	clk.Set(100_000_000_000)
	if _, err := svc.RunMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
	bal, _ = svc.GetBalance(context.Background(), "tnt", "tb")
	if bal.Balance != 10 {
		t.Fatalf("capped balance = %d, want 10", bal.Balance)
	}

	// Record entries before restart at this time.
	entriesBefore, _ := svc.ListLedger(context.Background(), "tnt", 0, 1000)
	balBefore, _ := svc.GetBalance(context.Background(), "tnt", "tb")

	// Close and restart at the same logical time using the same DB file.
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
	balAfter, _ := svc2.GetBalance(context.Background(), "tnt", "tb")
	if balAfter.Balance != balBefore.Balance {
		t.Fatalf("restart balance = %d, want %d", balAfter.Balance, balBefore.Balance)
	}
	entriesAfter, _ := svc2.ListLedger(context.Background(), "tnt", 0, 1000)
	if len(entriesAfter) != len(entriesBefore) {
		t.Fatalf("restart entry count = %d, want %d", len(entriesAfter), len(entriesBefore))
	}
	for i := range entriesBefore {
		if entriesBefore[i].Seq != entriesAfter[i].Seq {
			t.Fatalf("seq mismatch at %d: %d vs %d", i, entriesBefore[i].Seq, entriesAfter[i].Seq)
		}
		if entriesBefore[i].Type != entriesAfter[i].Type {
			t.Fatalf("type mismatch at %d", i)
		}
		if entriesBefore[i].Amount != entriesAfter[i].Amount {
			t.Fatalf("amount mismatch at %d: %d vs %d", i, entriesBefore[i].Amount, entriesAfter[i].Amount)
		}
	}
}

func TestReservationTriggeredRefillLedgerConsistency(t *testing.T) {
	svc, clk, _ := newTestService(t)
	mustCreateTokenBucket(t, svc, "tnt", "tb", 10, 0, 5, 1000)

	clk.AdvanceDuration(1_000_000_000)
	resp := mustReserve(t, svc, ReserveRequest{
		TenantID: "tnt",
		Items:    []Item{{Resource: "tb", Amount: 1}},
	})
	if len(resp.Balances) != 1 {
		t.Fatalf("balances = %d, want 1", len(resp.Balances))
	}
	bal := resp.Balances[0]
	if bal.Balance != 5 || bal.Frozen != 1 || bal.Available != 4 {
		t.Fatalf("balance/frozen/available = %d/%d/%d, want 5/1/4", bal.Balance, bal.Frozen, bal.Available)
	}

	entries, err := svc.ListLedger(context.Background(), "tnt", 0, 1000)
	if err != nil {
		t.Fatalf("list ledger: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("ledger entries = %d, want 3", len(entries))
	}
	if entries[1].Type != string(domain.EntryRefill) || entries[1].Amount != 5 {
		t.Fatalf("reservation-triggered entry = %+v, want refill amount 5", entries[1])
	}
	if entries[2].Type != string(domain.EntryReserve) || entries[2].Amount != 1 {
		t.Fatalf("entry after refill = %+v, want reserve amount 1", entries[2])
	}

	rec, err := svc.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !rec.OK {
		t.Fatalf("reconcile not OK: %+v", rec.Mismatches)
	}
	if err := svc.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if svc.Faulted() {
		t.Fatalf("service faulted after recovery: %s", svc.FaultReason())
	}
}

// Acceptance #6: cycle policy with explicit anchor across multiple cycles;
// run-maintenance produces only necessary, stably-sorted reset records and
// re-running at the same water mark does not reset again.
func TestAcceptance6_CycleResetsStableAndIdempotent(t *testing.T) {
	svc, clk, _ := newTestService(t)
	// Anchor at t=0, cycle length 1s.
	mustCreateTenant(t, svc, "tnt")
	_, err := svc.CreatePolicy(context.Background(), "tnt", "cyc", PolicySpec{
		Type:          string(domain.PolicyCycle),
		CycleLimit:    10,
		CycleLengthMS: 1000,
		CycleAnchorNS: 0,
	})
	if err != nil {
		t.Fatalf("create cycle: %v", err)
	}
	// Consume 6, leaving balance 4.
	if _, err := svc.Deduct(context.Background(), DeductRequest{
		TenantID: "tnt",
		Items:    []Item{{Resource: "cyc", Amount: 6}},
	}); err != nil {
		t.Fatal(err)
	}

	// Advance across 3 cycles.
	clk.Set(3_000_000_000)
	m1, err := svc.RunMaintenance(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Exactly one cycle_reset (balance was 4 < 10), amount 6.
	resets := 0
	for _, e := range m1.Entries {
		if e.Type == string(domain.EntryCycleReset) {
			resets++
			if e.Amount != 6 {
				t.Fatalf("reset amount = %d, want 6", e.Amount)
			}
		}
	}
	if resets != 1 {
		t.Fatalf("resets = %d, want 1", resets)
	}
	bal, _ := svc.GetBalance(context.Background(), "tnt", "cyc")
	if bal.Balance != 10 {
		t.Fatalf("balance = %d, want 10", bal.Balance)
	}

	// Re-run at same water mark: no new resets.
	m2, err := svc.RunMaintenance(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range m2.Entries {
		if e.Type == string(domain.EntryCycleReset) {
			t.Fatalf("unexpected reset on re-run: %+v", e)
		}
	}
}

// Acceptance #7: reservation with expiry; after restart with the clock advanced,
// recovery releases the quota exactly once and marks the reservation expired; a
// second restart adds no further entries.
func TestAcceptance7_RecoveryExpiresOnce(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "quotaraft.db")

	clk := clock.NewManual(1_000_000_000)
	st, err := store.New(store.Options{Path: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	svc := New(st, clk, WithIDGenerator(NewCounterIDGenerator("t")))
	if err := svc.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustCreateCyclePolicy(t, svc, "tnt", "r", 100)
	res := mustReserve(t, svc, ReserveRequest{
		TenantID:  "tnt",
		Items:     []Item{{Resource: "r", Amount: 40}},
		ExpiresAt: int64(clk.Now() + 1_000_000_000),
	})
	entriesBeforeClose, _ := svc.ListLedger(context.Background(), "tnt", 0, 1000)

	// Close, advance the clock past expiry, reopen with the same DB.
	_ = st.Close()
	clk.AdvanceDuration(5_000_000_000)
	st2, err := store.New(store.Options{Path: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	svc2 := New(st2, clk, WithIDGenerator(NewCounterIDGenerator("t")))
	if err := svc2.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}

	// Reservation expired, frozen released.
	bal, _ := svc2.GetBalance(context.Background(), "tnt", "r")
	if bal.Frozen != 0 {
		t.Fatalf("frozen = %d, want 0", bal.Frozen)
	}
	if bal.Balance != 100 {
		t.Fatalf("balance = %d, want 100", bal.Balance)
	}
	r, _ := svc2.GetReservation(context.Background(), "tnt", res.ReservationID)
	if r.Status != domain.StatusExpired {
		t.Fatalf("status = %s, want expired", r.Status)
	}

	entriesAfterRecover, _ := svc2.ListLedger(context.Background(), "tnt", 0, 1000)
	// Exactly one expiry_release entry added.
	added := len(entriesAfterRecover) - len(entriesBeforeClose)
	if added != 1 {
		t.Fatalf("entries added on recover = %d, want 1", added)
	}
	if entriesAfterRecover[len(entriesAfterRecover)-1].Type != string(domain.EntryExpiryRelease) {
		t.Fatalf("last entry = %s, want expiry_release", entriesAfterRecover[len(entriesAfterRecover)-1].Type)
	}

	// Second restart: no further entries.
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
	entriesAfterRecover2, _ := svc3.ListLedger(context.Background(), "tnt", 0, 1000)
	if len(entriesAfterRecover2) != len(entriesAfterRecover) {
		t.Fatalf("second restart added entries: %d vs %d", len(entriesAfterRecover2), len(entriesAfterRecover))
	}
}

// Acceptance #8: injected commit failure leaves no half-written state; retry
// with the same idempotency key yields a unique, correct result.
func TestAcceptance8_FaultInjectionAtomicity(t *testing.T) {
	st, err := store.New(store.Options{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	fi := store.NewFaultInjector(st)
	clk := clock.NewManual(1_000_000_000)
	svc := New(fi, clk, WithIDGenerator(NewCounterIDGenerator("t")))
	if err := svc.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustCreateCyclePolicy(t, svc, "tnt", "r", 100)
	res := mustReserve(t, svc, ReserveRequest{
		TenantID: "tnt",
		Items:    []Item{{Resource: "r", Amount: 40}},
	})

	// Inject a commit failure.
	fi.FailNextCommit(true)
	_, err = svc.Commit(context.Background(), CommitRequest{
		ReservationID:  res.ReservationID,
		TenantID:       "tnt",
		IdempotencyKey: "k",
		Usage:          map[string]int64{"r": 10},
	})
	if err == nil || err != fi.InjectedError() {
		t.Fatalf("expected injected fault, got %v", err)
	}
	fi.FailNextCommit(false)

	// No half-written state: reservation still pending, balance unchanged.
	r, _ := svc.GetReservation(context.Background(), "tnt", res.ReservationID)
	if r.Status != domain.StatusPending {
		t.Fatalf("status = %s, want pending (no partial commit)", r.Status)
	}
	bal, _ := svc.GetBalance(context.Background(), "tnt", "r")
	if bal.Balance != 100 {
		t.Fatalf("balance = %d, want 100 (no partial charge)", bal.Balance)
	}

	// Retry with the same key: succeeds with a unique result.
	resp, err := svc.Commit(context.Background(), CommitRequest{
		ReservationID:  res.ReservationID,
		TenantID:       "tnt",
		IdempotencyKey: "k",
		Usage:          map[string]int64{"r": 10},
	})
	if err != nil {
		t.Fatalf("retry commit: %v", err)
	}
	if resp.Status != string(domain.StatusCommitted) {
		t.Fatalf("status = %s, want committed", resp.Status)
	}
	bal, _ = svc.GetBalance(context.Background(), "tnt", "r")
	if bal.Balance != 90 {
		t.Fatalf("balance = %d, want 90", bal.Balance)
	}

	// One commit_charge and one release_unused in the ledger (no duplicates).
	entries, _ := svc.ListLedger(context.Background(), "tnt", 0, 1000)
	charges, releases := 0, 0
	for _, e := range entries {
		if e.Type == string(domain.EntryCommitCharge) {
			charges++
		}
		if e.Type == string(domain.EntryReleaseUnused) {
			releases++
		}
	}
	if charges != 1 || releases != 1 {
		t.Fatalf("charges=%d releases=%d, want 1/1", charges, releases)
	}
}

// Acceptance #9: rebuild matches online; tampering projection faults the
// service into read-only while queries still work.
func TestAcceptance9_ReconcileAndFault(t *testing.T) {
	svc, clk, _ := newTestService(t)
	clk.Set(1_000_000_000)
	mustCreateCyclePolicy(t, svc, "tnt", "r", 100)
	if _, err := svc.Deduct(context.Background(), DeductRequest{
		TenantID: "tnt",
		Items:    []Item{{Resource: "r", Amount: 30}},
	}); err != nil {
		t.Fatal(err)
	}

	// Rebuild matches online.
	rec, err := svc.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !rec.OK {
		t.Fatalf("reconcile not OK: %+v", rec.Mismatches)
	}

	// Tamper with the balance projection directly.
	if err := svc.store.Update(context.Background(), func(tx store.Tx) error {
		b, err := tx.GetBalance(context.Background(), "tnt", "r")
		if err != nil {
			return err
		}
		b.Balance = 999 // tamper
		return tx.UpsertBalance(context.Background(), b)
	}); err != nil {
		t.Fatal(err)
	}

	// Reconcile detects the mismatch.
	rec, err = svc.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rec.OK {
		t.Fatal("expected mismatch after tampering")
	}

	// Recover should fault the service.
	if err := svc.Recover(context.Background()); err != nil {
		// Recover returns the fault error itself; that's acceptable.
		_ = err
	}
	if !svc.Faulted() {
		t.Fatal("expected faulted state after tampering")
	}

	// Mutations rejected with CodeReadonly.
	_, err = svc.Deduct(context.Background(), DeductRequest{
		TenantID: "tnt",
		Items:    []Item{{Resource: "r", Amount: 1}},
	})
	if AsCode(err) != CodeReadonly {
		t.Fatalf("expected CodeReadonly, got %v", err)
	}

	// Queries and diagnostics still work.
	_, err = svc.GetBalance(context.Background(), "tnt", "r")
	if err != nil {
		t.Fatalf("query should still work when faulted: %v", err)
	}
	_, err = svc.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("diagnostics should still work when faulted: %v", err)
	}

	// ClearFault rebuilds and re-enables mutations.
	if err := svc.ClearFault(context.Background()); err != nil {
		t.Fatalf("clear fault: %v", err)
	}
	if svc.Faulted() {
		t.Fatal("expected not faulted after clear")
	}
}
