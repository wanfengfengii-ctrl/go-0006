package service

import (
	"github.com/quotaraft/quotaraft/internal/clock"
	"github.com/quotaraft/quotaraft/internal/domain"
)

// BalanceSnapshot is the public view of a balance at a point in time.
type BalanceSnapshot struct {
	Resource      string `json:"resource"`
	Balance       int64  `json:"balance"`
	Frozen        int64  `json:"frozen"`
	Available     int64  `json:"available"`
	PolicyVersion int64  `json:"policy_version"`
	PolicyType    string `json:"policy_type"`
}

// LedgerEntryView is the public view of a ledger entry.
type LedgerEntryView struct {
	Seq            int64  `json:"seq"`
	TenantID       string `json:"tenant_id"`
	Type           string `json:"type"`
	Resource       string `json:"resource"`
	Amount         int64  `json:"amount"`
	PolicyVersion  int64  `json:"policy_version"`
	CycleNumber    int64  `json:"cycle_number"`
	ReservationID  string `json:"reservation_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	Ts             int64  `json:"ts"`
}

func entryToView(e domain.LedgerEntry) LedgerEntryView {
	return LedgerEntryView{
		Seq:            e.Seq,
		TenantID:       e.TenantID,
		Type:           string(e.Type),
		Resource:       e.Resource,
		Amount:         e.Amount,
		PolicyVersion:  e.PolicyVersion,
		CycleNumber:    e.CycleNumber,
		ReservationID:  e.ReservationID,
		IdempotencyKey: e.IdempotencyKey,
		Ts:             int64(e.Ts),
	}
}

// Item is a single resource dimension in a deduct or reserve request.
type Item struct {
	Resource string `json:"resource"`
	Amount   int64  `json:"amount"`
}

// DeductRequest is an instant, atomic multi-resource deduction.
type DeductRequest struct {
	TenantID     string         `json:"tenant_id"`
	IdempotencyKey string       `json:"idempotency_key,omitempty"`
	// ExpectedPolicyVersions optionally pins the expected policy version per
	// resource; a mismatch yields CodePolicyVersionConflict.
	ExpectedPolicyVersions map[string]int64 `json:"expected_policy_versions,omitempty"`
	Items                   []Item           `json:"items"`
}

// DeductResponse is the result of a successful deduction.
type DeductResponse struct {
	TenantID   string            `json:"tenant_id"`
	Balances   []BalanceSnapshot `json:"balances"`
	Entries    []LedgerEntryView `json:"entries"`
	Idempotent bool              `json:"idempotent"`
}

// ReserveRequest creates a pending reservation freezing quota atomically.
type ReserveRequest struct {
	TenantID       string         `json:"tenant_id"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	ExpiresAt      int64          `json:"expires_at,omitempty"` // unix nanos; 0 = no expiry
	Items          []Item         `json:"items"`
}

// ReserveResponse is the result of a successful reservation.
type ReserveResponse struct {
	ReservationID string            `json:"reservation_id"`
	TenantID      string            `json:"tenant_id"`
	Status        string            `json:"status"`
	Balances      []BalanceSnapshot `json:"balances"`
	Entries       []LedgerEntryView `json:"entries"`
	Idempotent    bool              `json:"idempotent"`
}

// CommitRequest commits a pending reservation with actual usage per resource.
type CommitRequest struct {
	ReservationID string         `json:"reservation_id"`
	TenantID      string         `json:"tenant_id"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
	// Usage is the actual usage per resource. If a reserved resource is
	// omitted, usage defaults to zero (the whole frozen amount is released).
	Usage map[string]int64 `json:"usage"`
}

// CommitResponse is the result of a successful commit.
type CommitResponse struct {
	ReservationID string            `json:"reservation_id"`
	TenantID      string            `json:"tenant_id"`
	Status        string            `json:"status"`
	Balances      []BalanceSnapshot `json:"balances"`
	Entries       []LedgerEntryView `json:"entries"`
	Idempotent    bool              `json:"idempotent"`
}

// RollbackRequest rolls back a pending reservation.
type RollbackRequest struct {
	ReservationID  string `json:"reservation_id"`
	TenantID       string `json:"tenant_id"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// RollbackResponse is the result of a successful rollback.
type RollbackResponse struct {
	ReservationID string            `json:"reservation_id"`
	TenantID      string            `json:"tenant_id"`
	Status        string            `json:"status"`
	Balances      []BalanceSnapshot `json:"balances"`
	Entries       []LedgerEntryView `json:"entries"`
	Idempotent    bool              `json:"idempotent"`
}

// PolicySpec is the input for creating or updating a policy.
type PolicySpec struct {
	Type string `json:"type"`

	// Token-bucket fields.
	Capacity      int64 `json:"capacity"`
	InitialTokens int64 `json:"initial_tokens"`
	RefillAmount  int64 `json:"refill_amount"`
	// RefillIntervalMS is the refill interval in milliseconds.
	RefillIntervalMS int64 `json:"refill_interval_ms"`

	// Cycle fields.
	CycleLimit int64 `json:"cycle_limit"`
	// CycleLengthMS is the cycle length in milliseconds.
	CycleLengthMS int64 `json:"cycle_length_ms"`
	// CycleAnchorNS is the cycle anchor in unix nanoseconds.
	CycleAnchorNS int64 `json:"cycle_anchor_ns"`
}

// MaintenanceResult summarizes one maintenance pass.
type MaintenanceResult struct {
	ExpiredCount    int `json:"expired_count"`
	RefillCount     int `json:"refill_count"`
	CycleResetCount int `json:"cycle_reset_count"`
	Entries         []LedgerEntryView `json:"entries"`
}

// ReconcileResult is the outcome of a projection rebuild and comparison.
type ReconcileResult struct {
	OK      bool             `json:"ok"`
	Mismatches []Mismatch     `json:"mismatches,omitempty"`
}

// Mismatch describes a projection that does not match the rebuilt ledger.
type Mismatch struct {
	TenantID string `json:"tenant_id"`
	Resource string `json:"resource"`
	Field    string `json:"field"`
	Online   int64  `json:"online"`
	Rebuilt  int64  `json:"rebuilt"`
}

// MaintenanceConfig controls the coordinator.
type MaintenanceConfig struct {
	Enabled bool
	// MinInterval is the minimum time between automatic maintenance passes.
	MinInterval clock.Duration
}

// IDGenerator produces unique identifiers for reservations.
type IDGenerator interface {
	NewID() string
}
