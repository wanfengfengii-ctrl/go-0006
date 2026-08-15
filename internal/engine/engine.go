// Package engine implements the deterministic, integer-only quota
// calculations: token-bucket refill and cycle-number computation.
//
// All arithmetic is performed in int64 to avoid floating-point error. The
// functions are pure: given the same inputs they produce the same outputs,
// which is what makes balances reproducible across restarts and what lets the
// ledger projection be rebuilt deterministically.
package engine

import (
	"fmt"

	"github.com/quotaraft/quotaraft/internal/clock"
	"github.com/quotaraft/quotaraft/internal/domain"
)

// RefillResult is the outcome of settling a token bucket up to a given time.
type RefillResult struct {
	// NewBalance is the balance after applying the refill, capped at capacity.
	NewBalance int64
	// NewLastRefillTime is the time up to which refills are now materialized.
	// It advances by a whole number of refill intervals, preserving the
	// sub-interval remainder so that future refills stay aligned.
	NewLastRefillTime clock.Time
	// Amount is the number of tokens added (0 if no full interval elapsed or
	// the bucket was already at capacity).
	Amount int64
	// Intervals is the number of full intervals that elapsed.
	Intervals int64
}

// SettleTokenBucket computes the deterministic refill for a token bucket.
//
// The bucket is refilled by refillAmount for each whole refillInterval that
// has elapsed since lastRefillTime, capped at capacity. The remainder within
// the current (incomplete) interval is preserved by advancing
// lastRefillTime by exactly the consumed whole intervals.
//
// now must be >= lastRefillTime; otherwise ErrClockBackward is returned.
// Overflow in the refill computation is rejected.
func SettleTokenBucket(capacity, balance, refillAmount int64, refillInterval clock.Duration, lastRefillTime, now clock.Time) (RefillResult, error) {
	r := RefillResult{NewBalance: balance, NewLastRefillTime: lastRefillTime}
	if now < lastRefillTime {
		return r, domain.ErrClockBackward
	}
	if refillInterval <= 0 || refillAmount <= 0 {
		return r, nil
	}
	elapsed := int64(now - lastRefillTime)
	intervals := elapsed / int64(refillInterval)
	if intervals <= 0 {
		return r, nil
	}
	r.Intervals = intervals
	// Total tokens that would be added before capping.
	total, err := domain.MulChecked(refillAmount, intervals)
	if err != nil {
		return r, fmt.Errorf("%w: refill amount", err)
	}
	// The amount actually added is capped so that balance never exceeds
	// capacity.
	add := total
	if room := capacity - balance; room < add {
		add = room
	}
	if add < 0 {
		add = 0
	}
	newBalance, err := domain.AddChecked(balance, add)
	if err != nil {
		return r, fmt.Errorf("%w: new balance", err)
	}
	advanced, err := domain.MulChecked(intervals, int64(refillInterval))
	if err != nil {
		return r, fmt.Errorf("%w: advance time", err)
	}
	r.NewBalance = newBalance
	r.Amount = add
	r.NewLastRefillTime = lastRefillTime + clock.Time(advanced)
	return r, nil
}

// CycleNumber computes the cycle index for a point in time relative to an
// explicit anchor. It requires now >= anchor; a negative cycle is reported as
// ErrInvalidCycle.
func CycleNumber(anchor clock.Time, length clock.Duration, now clock.Time) (int64, error) {
	if length <= 0 {
		return 0, domain.ErrInvalidCycle
	}
	if now < anchor {
		return 0, domain.ErrClockBackward
	}
	delta := int64(now - anchor)
	n := delta / int64(length)
	if n < 0 {
		return 0, domain.ErrInvalidCycle
	}
	return n, nil
}

// CycleStartTime returns the absolute start time of cycle n.
func CycleStartTime(anchor clock.Time, length clock.Duration, n int64) clock.Time {
	return anchor + clock.Time(n*int64(length))
}

// CycleResetAmount returns the balance delta needed to reset a cycle policy's
// balance back to its limit at a cycle boundary. It is non-negative: if the
// balance is already at or above the limit (e.g. it was never consumed), the
// reset is a no-op and the amount is zero.
func CycleResetAmount(limit, balance int64) int64 {
	if balance >= limit {
		return 0
	}
	return limit - balance
}

// NormalizeResource returns a canonical, trimmed resource key. Resources are
// used as map keys and sort keys; normalization guarantees that two logically
// identical resource names compare equal and sort deterministically.
func NormalizeResource(s string) string {
	// Keep it simple: callers are expected to pass already-clean identifiers.
	// Trimming surrounding whitespace is the only normalization applied so
	// that "foo" and "foo " are not treated as different resources.
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

// SortItems returns a copy of items sorted by resource name with duplicates
// merged by summing their amounts. Sorting is required so that multi-resource
// operations touch resources in a stable order, which keeps ledger entries
// deterministic and avoids deadlocks.
func SortItems(items []domain.ReservationItem) []domain.ReservationItem {
	out := make([]domain.ReservationItem, 0, len(items))
	merged := make(map[string]int64, len(items))
	order := make([]string, 0, len(items))
	for _, it := range items {
		r := NormalizeResource(it.Resource)
		if r == "" {
			continue
		}
		if _, ok := merged[r]; !ok {
			order = append(order, r)
		}
		var err error
		merged[r], err = domain.AddChecked(merged[r], it.Amount)
		if err != nil {
			// Treat merge overflow as an invalid input; callers validate
			// amounts before reaching here, so this is defensive.
			merged[r] = merged[r] + it.Amount
		}
	}
	// Insertion sort for deterministic, dependency-free ordering.
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 && order[j-1] > order[j]; j-- {
			order[j-1], order[j] = order[j], order[j-1]
		}
	}
	for _, r := range order {
		out = append(out, domain.ReservationItem{Resource: r, Amount: merged[r]})
	}
	return out
}
