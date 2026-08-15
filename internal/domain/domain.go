// Package domain defines the core data models of QuotaRaft and the rules that
// give each ledger entry type a deterministic effect on the balance and frozen
// projections.
//
// A central design choice is that every balance change is captured by an
// immutable LedgerEntry. The balance and frozen amounts stored in the
// Balance projection are exactly the running sum of entry effects applied in
// sequence order, starting from zero. This makes the projection rebuildable:
// the online balance can always be checked against a fresh replay of the
// ledger, and tampering with the projection is detectable.
package domain

import (
	"errors"
	"fmt"
	"math"

	"github.com/quotaraft/quotaraft/internal/clock"
)

// PolicyType enumerates the supported quota strategies.
type PolicyType string

const (
	// PolicyTokenBucket is a token-bucket policy: a bucket of capacity tokens
	// that is lazily refilled by refill_amount every refill_interval.
	PolicyTokenBucket PolicyType = "token_bucket"
	// PolicyCycle is a fixed-cycle policy: a limit of tokens that resets to
	// the limit at each cycle boundary computed from an explicit anchor.
	PolicyCycle PolicyType = "cycle"
)

// EntryType enumerates the kinds of immutable ledger entries.
type EntryType string

const (
	EntryConsume         EntryType = "consume"          // instant deduction
	EntryReserve         EntryType = "reserve"          // freeze quota for a reservation
	EntryCommitCharge    EntryType = "commit_charge"    // charge actual usage on commit
	EntryReleaseUnused   EntryType = "release_unused"   // return unused frozen amount on commit
	EntryRollbackRelease EntryType = "rollback_release" // return frozen amount on rollback
	EntryExpiryRelease   EntryType = "expiry_release"   // return frozen amount on expiry
	EntryRefill          EntryType = "refill"           // token-bucket refill
	EntryCycleReset      EntryType = "cycle_reset"      // cycle boundary reset to limit
)

// Status enumerates reservation lifecycle states. Pending is the only
// non-terminal state; the remaining three are terminal and mutually exclusive.
type Status string

const (
	StatusPending     Status = "pending"
	StatusCommitted   Status = "committed"
	StatusRolledBack  Status = "rolled_back"
	StatusExpired     Status = "expired"
)

// IsTerminal reports whether the status is a terminal reservation state.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusCommitted, StatusRolledBack, StatusExpired:
		return true
	default:
		return false
	}
}

// Policy describes a quota policy version. A policy is identified by the
// (TenantID, Resource) pair; the Version field distinguishes successive
// revisions of the same policy.
type Policy struct {
	TenantID string
	Resource string
	Version  int64
	Type     PolicyType

	// Token-bucket fields (zero for cycle policies).
	Capacity       int64
	InitialTokens  int64
	RefillAmount   int64
	RefillInterval clock.Duration

	// Cycle fields (zero for token-bucket policies).
	CycleLimit   int64
	CycleLength  clock.Duration
	CycleAnchor  clock.Time

	CreatedAt clock.Time
}

// InitialBalance returns the balance a brand-new projection starts at for this
// policy: initial_tokens for a token bucket, the cycle limit for a cycle
// policy.
func (p Policy) InitialBalance() int64 {
	switch p.Type {
	case PolicyTokenBucket:
		return p.InitialTokens
	case PolicyCycle:
		return p.CycleLimit
	default:
		return 0
	}
}

// Tenant is a quota tenant.
type Tenant struct {
	ID        string
	CreatedAt clock.Time
}

// Balance is the rebuildable projection of the current settled balance and the
// amount frozen by pending reservations. Available = Balance - Frozen.
type Balance struct {
	TenantID      string
	Resource      string
	PolicyVersion int64
	Balance       int64
	Frozen        int64
	// LastRefillTime is the time up to which token-bucket refills have been
	// materialized. For cycle policies it is unused.
	LastRefillTime clock.Time
	// CurrentCycle is the cycle number that the projection currently reflects.
	// For token-bucket policies it is unused.
	CurrentCycle int64
}

// Available returns the spendable amount: Balance - Frozen.
func (b Balance) Available() int64 {
	return b.Balance - b.Frozen
}

// ReservationItem is one resource dimension of a reservation.
type ReservationItem struct {
	Resource string
	Amount   int64
}

// Reservation is a freeze of quota across one or more resources, awaiting
// commit or rollback.
type Reservation struct {
	ID        string
	TenantID  string
	Status    Status
	Items     []ReservationItem
	CreatedAt clock.Time
	ExpiresAt clock.Time // zero means no expiry
}

// HasExpiry reports whether the reservation expires.
func (r Reservation) HasExpiry() bool {
	return r.ExpiresAt != 0
}

// IsExpired reports whether the reservation is expired as of now.
func (r Reservation) IsExpired(now clock.Time) bool {
	return r.HasExpiry() && now >= r.ExpiresAt
}

// LedgerEntry is an immutable record of a single balance change.
type LedgerEntry struct {
	Seq          int64
	TenantID     string
	Type         EntryType
	Resource     string
	Amount       int64
	PolicyVersion int64
	CycleNumber  int64
	ReservationID string
	IdempotencyKey string
	Ts           clock.Time
}

// BalanceDelta returns the signed effect an entry has on the Balance.Balance
// projection. Positive increases the balance.
func (e LedgerEntry) BalanceDelta() int64 {
	switch e.Type {
	case EntryConsume:
		return -e.Amount
	case EntryReserve:
		return 0
	case EntryCommitCharge:
		return -e.Amount
	case EntryReleaseUnused:
		return 0
	case EntryRollbackRelease:
		return 0
	case EntryExpiryRelease:
		return 0
	case EntryRefill:
		return e.Amount
	case EntryCycleReset:
		return e.Amount
	default:
		return 0
	}
}

// FrozenDelta returns the signed effect an entry has on the Balance.Frozen
// projection. Positive increases frozen (decreases available).
func (e LedgerEntry) FrozenDelta() int64 {
	switch e.Type {
	case EntryConsume:
		return 0
	case EntryReserve:
		return e.Amount
	case EntryCommitCharge:
		// The used portion leaves the frozen pool and becomes a real charge,
		// so it reduces frozen by the charged amount.
		return -e.Amount
	case EntryReleaseUnused:
		return -e.Amount
	case EntryRollbackRelease:
		return -e.Amount
	case EntryExpiryRelease:
		return -e.Amount
	case EntryRefill:
		return 0
	case EntryCycleReset:
		return 0
	default:
		return 0
	}
}

// CoordinatorState persists the maintenance water mark and the service fault
// state across restarts.
type CoordinatorState struct {
	LastMaintenance clock.Time
	Faulted         bool
	FaultReason     string
}

// Sentinel errors returned by validation helpers.
var (
	ErrNegativeAmount   = errors.New("quotaraft: amount must be non-negative")
	ErrOverflow         = errors.New("quotaraft: integer overflow")
	ErrInvalidPolicy    = errors.New("quotaraft: invalid policy")
	ErrInvalidCycle     = errors.New("quotaraft: invalid cycle configuration")
	ErrClockBackward    = errors.New("quotaraft: clock moved backwards")
)

// ValidateAmount rejects negative amounts and detects overflow when an amount
// is added to a running total.
func ValidateAmount(amount int64) error {
	if amount < 0 {
		return ErrNegativeAmount
	}
	return nil
}

// AddChecked adds a and b, returning ErrOverflow on 64-bit overflow.
func AddChecked(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, ErrOverflow
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, ErrOverflow
	}
	return a + b, nil
}

// MulChecked multiplies a and b, returning ErrOverflow on overflow.
func MulChecked(a, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	r := a * b
	if r/a != b {
		return 0, ErrOverflow
	}
	return r, nil
}

// ValidatePolicy checks that a policy is self-consistent. It rejects negative
// fields, non-positive intervals/lengths for the relevant type, and a cycle
// anchor that lies in the future relative to createdAt (which would yield a
// negative cycle number).
func ValidatePolicy(p Policy) error {
	if p.TenantID == "" || p.Resource == "" {
		return fmt.Errorf("%w: tenant and resource are required", ErrInvalidPolicy)
	}
	if p.Version < 1 {
		return fmt.Errorf("%w: version must be >= 1", ErrInvalidPolicy)
	}
	switch p.Type {
	case PolicyTokenBucket:
		if p.Capacity < 0 || p.InitialTokens < 0 || p.RefillAmount < 0 {
			return fmt.Errorf("%w: token-bucket fields must be non-negative", ErrInvalidPolicy)
		}
		if p.InitialTokens > p.Capacity {
			return fmt.Errorf("%w: initial_tokens exceeds capacity", ErrInvalidPolicy)
		}
		if p.RefillInterval <= 0 && p.RefillAmount > 0 {
			return fmt.Errorf("%w: refill_interval must be positive when refilling", ErrInvalidPolicy)
		}
	case PolicyCycle:
		if p.CycleLimit < 0 {
			return fmt.Errorf("%w: cycle_limit must be non-negative", ErrInvalidPolicy)
		}
		if p.CycleLength <= 0 {
			return fmt.Errorf("%w: cycle_length must be positive", ErrInvalidCycle)
		}
		if p.CreatedAt < p.CycleAnchor {
			return fmt.Errorf("%w: created_at precedes cycle anchor", ErrInvalidCycle)
		}
	default:
		return fmt.Errorf("%w: unknown policy type %q", ErrInvalidPolicy, p.Type)
	}
	return nil
}
