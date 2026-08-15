package domain

import (
	"math"
	"testing"

	"github.com/quotaraft/quotaraft/internal/clock"
)

func TestEntryDeltas(t *testing.T) {
	cases := []struct {
		typ      EntryType
		amount   int64
		wantBal  int64
		wantFroz int64
	}{
		{EntryConsume, 5, -5, 0},
		{EntryReserve, 5, 0, 5},
		{EntryCommitCharge, 3, -3, -3},
		{EntryReleaseUnused, 2, 0, -2},
		{EntryRollbackRelease, 5, 0, -5},
		{EntryExpiryRelease, 5, 0, -5},
		{EntryRefill, 4, 4, 0},
		{EntryCycleReset, 7, 7, 0},
	}
	for _, c := range cases {
		e := LedgerEntry{Type: c.typ, Amount: c.amount}
		if got := e.BalanceDelta(); got != c.wantBal {
			t.Errorf("%s BalanceDelta = %d, want %d", c.typ, got, c.wantBal)
		}
		if got := e.FrozenDelta(); got != c.wantFroz {
			t.Errorf("%s FrozenDelta = %d, want %d", c.typ, got, c.wantFroz)
		}
	}
}

func TestRebuildFromEntries(t *testing.T) {
	// A complete reservation lifecycle replayed from entries should reproduce
	// the same balance and frozen as the online projection.
	entries := []LedgerEntry{
		{Type: EntryRefill, Amount: 100},       // initial balance 100
		{Type: EntryReserve, Amount: 40},       // freeze 40
		{Type: EntryCommitCharge, Amount: 25},  // charge 25; used leaves frozen
		{Type: EntryReleaseUnused, Amount: 15}, // return unused 15 frozen
	}
	var bal, froz int64
	for _, e := range entries {
		bal += e.BalanceDelta()
		froz += e.FrozenDelta()
	}
	// balance: 100 - 25 = 75 ; frozen: 40 - 25 (charge) - 15 (release) = 0
	if bal != 75 {
		t.Fatalf("rebuild balance = %d, want 75", bal)
	}
	if froz != 0 {
		t.Fatalf("rebuild frozen = %d, want 0 (committed reservation fully released)", froz)
	}
}

func TestValidatePolicy(t *testing.T) {
	good := Policy{TenantID: "t", Resource: "r", Version: 1, Type: PolicyTokenBucket,
		Capacity: 10, InitialTokens: 5, RefillAmount: 1, RefillInterval: clock.Duration(1_000_000_000), CreatedAt: 100}
	if err := ValidatePolicy(good); err != nil {
		t.Fatalf("good policy rejected: %v", err)
	}
	bad := good
	bad.InitialTokens = 20
	if err := ValidatePolicy(bad); err == nil {
		t.Fatal("expected error for initial > capacity")
	}
	badCycle := Policy{TenantID: "t", Resource: "r", Version: 1, Type: PolicyCycle,
		CycleLimit: 10, CycleLength: clock.Duration(1_000_000_000), CycleAnchor: 200, CreatedAt: 100}
	if err := ValidatePolicy(badCycle); err == nil {
		t.Fatal("expected error for created_at before anchor")
	}
}

func TestAddMulChecked(t *testing.T) {
	if r, err := AddChecked((1<<62)-1, 1); err != nil || r != 1<<62 {
		t.Fatalf("AddChecked near max failed: %d %v", r, err)
	}
	if _, err := AddChecked(math.MaxInt64, 1); err != ErrOverflow {
		t.Fatalf("expected overflow at max, got %v", err)
	}
	if r, err := MulChecked(1<<31, 1<<31); err != nil || r != 1<<62 {
		t.Fatalf("MulChecked failed: %d %v", r, err)
	}
	if _, err := MulChecked((1<<40)+1, (1<<40)+1); err != ErrOverflow {
		t.Fatalf("expected overflow, got %v", err)
	}
}

func TestStatusTerminal(t *testing.T) {
	if StatusPending.IsTerminal() {
		t.Fatal("pending should not be terminal")
	}
	for _, s := range []Status{StatusCommitted, StatusRolledBack, StatusExpired} {
		if !s.IsTerminal() {
			t.Fatalf("%s should be terminal", s)
		}
	}
}

func TestReservationExpiry(t *testing.T) {
	r := Reservation{ExpiresAt: 100}
	if !r.HasExpiry() {
		t.Fatal("should have expiry")
	}
	if !r.IsExpired(100) {
		t.Fatal("should be expired at expiry time")
	}
	if r.IsExpired(99) {
		t.Fatal("should not be expired before expiry time")
	}
}
