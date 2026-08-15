package engine

import (
	"testing"

	"github.com/quotaraft/quotaraft/internal/clock"
	"github.com/quotaraft/quotaraft/internal/domain"
)

func TestSettleTokenBucketNoRefillBeforeInterval(t *testing.T) {
	// capacity 10, 1 token per second, last refill at t=0.
	r, err := SettleTokenBucket(10, 5, 1, clock.Duration(1_000_000_000), 0, 500_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if r.Amount != 0 || r.NewBalance != 5 || r.NewLastRefillTime != 0 {
		t.Fatalf("expected no refill before interval, got %+v", r)
	}
}

func TestSettleTokenBucketFullIntervals(t *testing.T) {
	// 2 full intervals: +2 tokens, remainder preserved.
	r, err := SettleTokenBucket(10, 5, 1, clock.Duration(1_000_000_000), 0, 2_500_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if r.Amount != 2 {
		t.Fatalf("amount = %d, want 2", r.Amount)
	}
	if r.NewBalance != 7 {
		t.Fatalf("balance = %d, want 7", r.NewBalance)
	}
	if r.NewLastRefillTime != 2_000_000_000 {
		t.Fatalf("last refill = %d, want 2e9", r.NewLastRefillTime)
	}
}

func TestSettleTokenBucketCapsAtCapacity(t *testing.T) {
	// balance already near capacity: only room for 1, even if intervals add more.
	r, err := SettleTokenBucket(10, 9, 5, clock.Duration(1_000_000_000), 0, 3_000_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if r.Amount != 1 {
		t.Fatalf("amount = %d, want 1 (capped)", r.Amount)
	}
	if r.NewBalance != 10 {
		t.Fatalf("balance = %d, want 10 (capacity)", r.NewBalance)
	}
	if r.NewLastRefillTime != 3_000_000_000 {
		t.Fatalf("last refill should advance by all intervals even when capped: %d", r.NewLastRefillTime)
	}
}

func TestSettleTokenBucketIdempotent(t *testing.T) {
	// Settling again at the same time after a settle should add nothing.
	r1, err := SettleTokenBucket(10, 5, 2, clock.Duration(1_000_000_000), 0, 3_000_000_000)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := SettleTokenBucket(10, r1.NewBalance, 2, clock.Duration(1_000_000_000), r1.NewLastRefillTime, 3_000_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Amount != 0 || r2.NewBalance != r1.NewBalance {
		t.Fatalf("re-settle not idempotent: %+v", r2)
	}
}

func TestSettleTokenBucketClockBackward(t *testing.T) {
	_, err := SettleTokenBucket(10, 5, 1, clock.Duration(1_000_000_000), 100, 50)
	if err == nil {
		t.Fatal("expected ErrClockBackward")
	}
}

func TestCycleNumber(t *testing.T) {
	anchor := clock.Time(0)
	length := clock.Duration(1_000_000_000)
	cases := []struct {
		now  clock.Time
		want int64
	}{
		{0, 0},
		{999_999_999, 0},
		{1_000_000_000, 1},
		{2_500_000_000, 2},
	}
	for _, c := range cases {
		got, err := CycleNumber(anchor, length, c.now)
		if err != nil {
			t.Fatalf("CycleNumber(%d): %v", c.now, err)
		}
		if got != c.want {
			t.Errorf("CycleNumber(%d) = %d, want %d", c.now, got, c.want)
		}
	}
}

func TestCycleResetAmount(t *testing.T) {
	if got := CycleResetAmount(10, 4); got != 6 {
		t.Fatalf("reset amount = %d, want 6", got)
	}
	if got := CycleResetAmount(10, 10); got != 0 {
		t.Fatalf("reset at limit = %d, want 0", got)
	}
	if got := CycleResetAmount(10, 12); got != 0 {
		t.Fatalf("reset above limit = %d, want 0", got)
	}
}

func TestSortItemsMergesAndSorts(t *testing.T) {
	items := []domain.ReservationItem{
		{Resource: "b", Amount: 2},
		{Resource: "a", Amount: 1},
		{Resource: "a", Amount: 3},
		{Resource: "", Amount: 99}, // dropped
		{Resource: "b", Amount: 0},
	}
	out := SortItems(items)
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2", len(out))
	}
	if out[0].Resource != "a" || out[0].Amount != 4 {
		t.Errorf("out[0] = %+v", out[0])
	}
	if out[1].Resource != "b" || out[1].Amount != 2 {
		t.Errorf("out[1] = %+v", out[1])
	}
}
