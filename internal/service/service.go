package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/quotaraft/quotaraft/internal/clock"
	"github.com/quotaraft/quotaraft/internal/domain"
	"github.com/quotaraft/quotaraft/internal/engine"
	"github.com/quotaraft/quotaraft/internal/store"
)

// Service is the quota ledger. It is safe for concurrent use: all mutations
// are serialized by the store's write transaction, and read methods use the
// store's view transactions.
type Service struct {
	store  store.Store
	clock  clock.Clock
	ids    IDGenerator
	config MaintenanceConfig

	// started is set after Recover has run successfully. While false, the
	// service rejects mutations with CodeInternal (not ready).
	started bool
	// faulted is set when a projection invariant check fails. While true the
	// service is read-only: queries and diagnostics still work, but mutations
	// are rejected with CodeReadonly.
	faulted         bool
	faultReasonCache string
}

// Option configures a Service.
type Option func(*Service)

// WithIDGenerator overrides the default reservation ID generator.
func WithIDGenerator(g IDGenerator) Option {
	return func(s *Service) { s.ids = g }
}

// WithMaintenanceConfig overrides the maintenance configuration.
func WithMaintenanceConfig(c MaintenanceConfig) Option {
	return func(s *Service) { s.config = c }
}

// New creates a Service. The store is not migrated or recovered; call Recover
// before serving traffic.
func New(st store.Store, clk clock.Clock, opts ...Option) *Service {
	s := &Service{
		store: st,
		clock: clk,
		ids:   &defaultIDGenerator{},
		config: MaintenanceConfig{
			Enabled:     true,
			MinInterval: clock.Duration(timeSecondNS),
		},
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// hashRequest computes a stable digest of an operation and its canonical
// request payload. The same logical request always yields the same digest.
func hashRequest(op string, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(op))
	h.Write([]byte{0})
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// canonicalJSON marshals v with map keys sorted so that identical requests
// produce identical bytes regardless of map iteration order.
func canonicalJSON(v any) ([]byte, error) {
	// json.Marshal already sorts struct fields by definition and map keys
	// alphabetically, which is sufficient for our request types.
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// validateItems normalizes request items: it rejects negative/overflowing
// amounts and returns merged, sorted items.
func validateItems(items []Item) ([]domain.ReservationItem, error) {
	out := make([]domain.ReservationItem, 0, len(items))
	for _, it := range items {
		if it.Amount < 0 {
			return nil, NewError(CodeInvalidInput, "amount must be non-negative")
		}
		if it.Resource == "" {
			return nil, NewError(CodeInvalidInput, "resource is required")
		}
		out = append(out, domain.ReservationItem{Resource: it.Resource, Amount: it.Amount})
	}
	merged := engine.SortItems(out)
	if len(merged) == 0 {
		return nil, NewError(CodeInvalidInput, "at least one item is required")
	}
	return merged, nil
}

// CreateTenant creates a tenant.
func (s *Service) CreateTenant(ctx context.Context, id string) error {
	if id == "" {
		return NewError(CodeInvalidInput, "tenant id is required")
	}
	now := s.clock.Now()
	return s.store.Update(ctx, func(tx store.Tx) error {
		return tx.UpsertTenant(ctx, domain.Tenant{ID: id, CreatedAt: now})
	})
}

// CreatePolicy creates a policy and initializes its balance projection plus an
// opening ledger entry (refill for token buckets, cycle_reset for cycles) so
// that the ledger fully explains the starting balance.
func (s *Service) CreatePolicy(ctx context.Context, tenantID, resource string, spec PolicySpec) (domain.Policy, error) {
	if tenantID == "" || resource == "" {
		return domain.Policy{}, NewError(CodeInvalidInput, "tenant and resource are required")
	}
	p, err := specToPolicy(tenantID, resource, 1, s.clock.Now(), spec)
	if err != nil {
		return domain.Policy{}, err
	}
	if err := domain.ValidatePolicy(p); err != nil {
		return domain.Policy{}, wrapDomainErr(err)
	}

	err = s.store.Update(ctx, func(tx store.Tx) error {
		// Ensure tenant exists.
		if _, err := tx.GetTenant(ctx, tenantID); err != nil {
			if store.IsNotFound(err) {
				return NewError(CodeNotFound, "tenant not found")
			}
			return err
		}
		// Reject if a policy already exists for this resource.
		if existing, err := tx.CurrentPolicy(ctx, tenantID, resource); err == nil && existing.Version >= 1 {
			return NewError(CodeInvalidInput, "policy already exists; use update")
		} else if err != nil && !store.IsNotFound(err) {
			return err
		}
		if err := tx.UpsertPolicy(ctx, p); err != nil {
			return err
		}
		// Initialize balance projection.
		bal := domain.Balance{
			TenantID:      tenantID,
			Resource:      resource,
			PolicyVersion: p.Version,
			Balance:       p.InitialBalance(),
		}
		if p.Type == domain.PolicyTokenBucket {
			bal.LastRefillTime = p.CreatedAt
		} else {
			cn, err := engine.CycleNumber(p.CycleAnchor, p.CycleLength, p.CreatedAt)
			if err != nil {
				return err
			}
			bal.CurrentCycle = cn
		}
		if err := tx.UpsertBalance(ctx, bal); err != nil {
			return err
		}
		// Opening ledger entry.
		var et domain.EntryType
		if p.Type == domain.PolicyTokenBucket {
			et = domain.EntryRefill
		} else {
			et = domain.EntryCycleReset
		}
		_, err := tx.InsertLedgerEntry(ctx, domain.LedgerEntry{
			TenantID:       tenantID,
			Type:           et,
			Resource:       resource,
			Amount:         p.InitialBalance(),
			PolicyVersion:  p.Version,
			CycleNumber:    bal.CurrentCycle,
			Ts:             p.CreatedAt,
		})
		return err
	})
	if err != nil {
		return domain.Policy{}, err
	}
	return p, nil
}

// UpdatePolicy creates a new version of an existing policy. If the new
// capacity/limit is below the current balance, the excess is truncated and
// recorded as a consume entry so the ledger stays consistent with the
// projection. Existing ledger entries keep their original policy version.
func (s *Service) UpdatePolicy(ctx context.Context, tenantID, resource string, spec PolicySpec) (domain.Policy, error) {
	if tenantID == "" || resource == "" {
		return domain.Policy{}, NewError(CodeInvalidInput, "tenant and resource are required")
	}
	now := s.clock.Now()
	var result domain.Policy
	err := s.store.Update(ctx, func(tx store.Tx) error {
		current, err := tx.CurrentPolicy(ctx, tenantID, resource)
		if err != nil {
			if store.IsNotFound(err) {
				return NewError(CodeNotFound, "policy not found")
			}
			return err
		}
		newVersion := current.Version + 1
		np, err := specToPolicy(tenantID, resource, newVersion, now, spec)
		if err != nil {
			return err
		}
		np.CreatedAt = current.CreatedAt // preserve original creation time for cycle math
		if err := domain.ValidatePolicy(np); err != nil {
			return wrapDomainErr(err)
		}
		if err := tx.UpsertPolicy(ctx, np); err != nil {
			return err
		}
		// Load and adjust the balance projection.
		bal, err := tx.GetBalance(ctx, tenantID, resource)
		if err != nil {
			if store.IsNotFound(err) {
				return NewError(CodeInternal, "balance missing for existing policy")
			}
			return err
		}
		bal.PolicyVersion = newVersion

		// Truncate balance to the new ceiling if necessary and record the
		// reduction so the rebuild stays consistent.
		var ceiling int64
		if np.Type == domain.PolicyTokenBucket {
			ceiling = np.Capacity
			// last_refill_time is preserved for token buckets.
		} else {
			ceiling = np.CycleLimit
			cn, err := engine.CycleNumber(np.CycleAnchor, np.CycleLength, now)
			if err != nil {
				return err
			}
			bal.CurrentCycle = cn
		}
		if bal.Balance > ceiling {
			excess := bal.Balance - ceiling
			if _, err := tx.InsertLedgerEntry(ctx, domain.LedgerEntry{
				TenantID:      tenantID,
				Type:          domain.EntryConsume,
				Resource:      resource,
				Amount:        excess,
				PolicyVersion: newVersion,
				CycleNumber:   bal.CurrentCycle,
				Ts:            now,
			}); err != nil {
				return err
			}
			bal.Balance = ceiling
		}
		result = np
		return tx.UpsertBalance(ctx, bal)
	})
	if err != nil {
		return domain.Policy{}, err
	}
	return result, nil
}

// GetPolicy returns the current policy for a resource.
func (s *Service) GetPolicy(ctx context.Context, tenantID, resource string) (domain.Policy, error) {
	var p domain.Policy
	err := s.store.View(ctx, func(tx store.Tx) error {
		var err error
		p, err = tx.CurrentPolicy(ctx, tenantID, resource)
		return err
	})
	if err != nil {
		return domain.Policy{}, err
	}
	return p, nil
}

// GetBalance returns the current balance projection for a resource. It does
// not settle refills (reads do not mutate); call RunMaintenance to materialize
// pending refills.
func (s *Service) GetBalance(ctx context.Context, tenantID, resource string) (BalanceSnapshot, error) {
	var snap BalanceSnapshot
	err := s.store.View(ctx, func(tx store.Tx) error {
		bal, err := tx.GetBalance(ctx, tenantID, resource)
		if err != nil {
			return err
		}
		p, err := tx.CurrentPolicy(ctx, tenantID, resource)
		if err != nil {
			return err
		}
		snap = balanceToSnapshot(bal, p)
		return nil
	})
	if err != nil {
		return BalanceSnapshot{}, err
	}
	return snap, nil
}

// ListBalances returns all balance projections for a tenant.
func (s *Service) ListBalances(ctx context.Context, tenantID string) ([]BalanceSnapshot, error) {
	var out []BalanceSnapshot
	err := s.store.View(ctx, func(tx store.Tx) error {
		bals, err := tx.ListBalances(ctx, tenantID)
		if err != nil {
			return err
		}
		for _, bal := range bals {
			p, err := tx.CurrentPolicy(ctx, tenantID, bal.Resource)
			if err != nil {
				return err
			}
			out = append(out, balanceToSnapshot(bal, p))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListLedger returns ledger entries for a tenant, paginated by sequence.
func (s *Service) ListLedger(ctx context.Context, tenantID string, afterSeq int64, limit int) ([]LedgerEntryView, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	var out []LedgerEntryView
	err := s.store.View(ctx, func(tx store.Tx) error {
		entries, err := tx.ListLedgerEntries(ctx, tenantID, afterSeq, limit)
		if err != nil {
			return err
		}
		for _, e := range entries {
			out = append(out, entryToView(e))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func balanceToSnapshot(bal domain.Balance, p domain.Policy) BalanceSnapshot {
	return BalanceSnapshot{
		Resource:      bal.Resource,
		Balance:       bal.Balance,
		Frozen:        bal.Frozen,
		Available:     bal.Available(),
		PolicyVersion: bal.PolicyVersion,
		PolicyType:    string(p.Type),
	}
}

func specToPolicy(tenantID, resource string, version int64, now clock.Time, spec PolicySpec) (domain.Policy, error) {
	p := domain.Policy{
		TenantID:        tenantID,
		Resource:        resource,
		Version:         version,
		CreatedAt:       now,
		RefillInterval:  clock.Duration(specMillisToNanos(spec.RefillIntervalMS)),
		CycleLength:     clock.Duration(specMillisToNanos(spec.CycleLengthMS)),
		CycleAnchor:     clock.Time(spec.CycleAnchorNS),
		Capacity:        spec.Capacity,
		InitialTokens:   spec.InitialTokens,
		RefillAmount:    spec.RefillAmount,
		CycleLimit:      spec.CycleLimit,
	}
	switch domain.PolicyType(spec.Type) {
	case "", domain.PolicyTokenBucket:
		p.Type = domain.PolicyTokenBucket
	case domain.PolicyCycle:
		p.Type = domain.PolicyCycle
	default:
		return domain.Policy{}, NewError(CodeInvalidInput, "unknown policy type: "+spec.Type)
	}
	if p.Type == domain.PolicyCycle && p.CycleAnchor == 0 {
		// Default the anchor to the creation time so callers do not have to
		// supply it explicitly; an explicit anchor always wins.
		p.CycleAnchor = now
	}
	return p, nil
}

func specMillisToNanos(ms int64) int64 { return ms * 1_000_000 }

func wrapDomainErr(err error) error {
	switch err {
	case domain.ErrOverflow:
		return NewError(CodeInputOverflow, "integer overflow")
	case domain.ErrNegativeAmount:
		return NewError(CodeInvalidInput, "amount must be non-negative")
	case domain.ErrInvalidCycle:
		return NewError(CodeInvalidInput, "invalid cycle configuration")
	case domain.ErrClockBackward:
		return NewError(CodeInvalidInput, "clock moved backwards")
	default:
		return NewError(CodeInvalidInput, err.Error())
	}
}

// settleTokenBucket lazily refills a token-bucket balance up to now and writes
// a refill entry if any tokens were added. It returns the updated balance.
// This is the shared lazy-settle routine used by mutating operations.
func settleTokenBucket(ctx context.Context, tx store.Tx, p domain.Policy, bal domain.Balance, now clock.Time) (domain.Balance, []domain.LedgerEntry, error) {
	if p.Type != domain.PolicyTokenBucket {
		return bal, nil, nil
	}
	res, err := engine.SettleTokenBucket(p.Capacity, bal.Balance, p.RefillAmount, p.RefillInterval, bal.LastRefillTime, now)
	if err != nil {
		return bal, nil, err
	}
	if res.Amount == 0 {
		// Even with no tokens added, advance last_refill_time if whole
		// intervals elapsed (e.g. bucket already at capacity) so future
		// settles do not recompute the same elapsed window.
		if res.NewLastRefillTime != bal.LastRefillTime {
			bal.LastRefillTime = res.NewLastRefillTime
		}
		return bal, nil, nil
	}
	entry := domain.LedgerEntry{
		TenantID:      p.TenantID,
		Type:          domain.EntryRefill,
		Resource:      p.Resource,
		Amount:        res.Amount,
		PolicyVersion: p.Version,
		Ts:            now,
	}
	bal.Balance = res.NewBalance
	bal.LastRefillTime = res.NewLastRefillTime
	return bal, []domain.LedgerEntry{entry}, nil
}

// idempotency wraps a mutation: it checks for an existing record, returns the
// cached response on a key+hash match, conflicts on a key+hash mismatch, and
// otherwise runs fn and caches its successful response. The type parameter T
// ensures the cached JSON is decoded back into the concrete response type.
func idempotency[T any](ctx context.Context, s *Service, tenantID, key, op string, req any, fn func(tx store.Tx) (*T, error)) (*T, bool, error) {
	payload, err := canonicalJSON(req)
	if err != nil {
		return nil, false, NewError(CodeInternal, "marshal request: "+err.Error())
	}
	hash := hashRequest(op, payload)

	var resp *T
	var idempotent bool
	txErr := s.store.Update(ctx, func(tx store.Tx) error {
		if key != "" {
			rec, err := tx.GetIdempotencyRecord(ctx, tenantID, key)
			if err == nil {
				// Existing record: match or conflict.
				if rec.RequestHash != hash {
					return NewError(CodeIdempotencyConflict, "idempotency key reused with different request")
				}
				var decoded T
				if err := json.Unmarshal(rec.Response, &decoded); err != nil {
					return NewError(CodeInternal, "unmarshal cached response: "+err.Error())
				}
				resp = &decoded
				idempotent = true
				return nil
			} else if !store.IsNotFound(err) {
				return err
			}
		}
		// No existing record: run the operation.
		out, err := fn(tx)
		if err != nil {
			return err
		}
		resp = out
		if key != "" {
			b, err := json.Marshal(out)
			if err != nil {
				return NewError(CodeInternal, "marshal response: "+err.Error())
			}
			if err := tx.InsertIdempotencyRecord(ctx, store.IdempotencyRecord{
				TenantID:    tenantID,
				Key:         key,
				RequestHash: hash,
				Response:    b,
				CreatedAt:   s.clock.Now(),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if txErr != nil {
		return nil, false, txErr
	}
	return resp, idempotent, nil
}

// Deduct performs an instant, atomic, multi-resource deduction.
func (s *Service) Deduct(ctx context.Context, req DeductRequest) (*DeductResponse, error) {
	items, err := validateItems(req.Items)
	if err != nil {
		return nil, err
	}
	if req.TenantID == "" {
		return nil, NewError(CodeInvalidInput, "tenant id is required")
	}
	if err := s.requireStarted(); err != nil {
		return nil, err
	}

	out, idempotent, err := idempotency(ctx, s, req.TenantID, req.IdempotencyKey, "deduct", req, func(tx store.Tx) (*DeductResponse, error) {
		return s.deductTx(ctx, tx, req.TenantID, items, req.ExpectedPolicyVersions)
	})
	if err != nil {
		return nil, err
	}
	resp := out
	resp.Idempotent = idempotent
	return resp, nil
}

func (s *Service) deductTx(ctx context.Context, tx store.Tx, tenantID string, items []domain.ReservationItem, expected map[string]int64) (*DeductResponse, error) {
	now := s.clock.Now()
	// Phase 1: settle and check availability for all dimensions without
	// writing consume entries. If any dimension is insufficient, the
	// transaction returns an error and nothing is persisted.
	type staged struct {
		p     domain.Policy
		bal   domain.Balance
		extra []domain.LedgerEntry // refill entries to write
	}
	stagedMap := make(map[string]staged, len(items))
	var sortedResources []string
	for _, it := range items {
		if _, ok := stagedMap[it.Resource]; ok {
			continue
		}
		p, err := tx.CurrentPolicy(ctx, tenantID, it.Resource)
		if err != nil {
			if store.IsNotFound(err) {
				return nil, NewError(CodeNotFound, "policy not found: "+it.Resource)
			}
			return nil, err
		}
		if v, ok := expected[it.Resource]; ok && v != 0 && v != p.Version {
			return nil, NewError(CodePolicyVersionConflict, fmt.Sprintf("policy version mismatch for %s: expected %d got %d", it.Resource, v, p.Version))
		}
		bal, err := tx.GetBalance(ctx, tenantID, it.Resource)
		if err != nil {
			if store.IsNotFound(err) {
				return nil, NewError(CodeInternal, "balance missing for policy")
			}
			return nil, err
		}
		bal, extra, err := settleTokenBucket(ctx, tx, p, bal, now)
		if err != nil {
			return nil, err
		}
		stagedMap[it.Resource] = staged{p: p, bal: bal, extra: extra}
		sortedResources = append(sortedResources, it.Resource)
	}
	sort.Strings(sortedResources)

	// Check availability for all dimensions first.
	for _, it := range items {
		st := stagedMap[it.Resource]
		if st.bal.Available() < it.Amount {
			return nil, NewError(CodeInsufficientQuota, fmt.Sprintf("insufficient quota for %s: need %d have %d", it.Resource, it.Amount, st.bal.Available()))
		}
	}

	// Phase 2: persist refill entries, consume entries, and updated balances.
	var allEntries []LedgerEntryView
	var snapshots []BalanceSnapshot
	for _, r := range sortedResources {
		st := stagedMap[r]
		// Write any refill entries first.
		if err := tx.UpsertBalance(ctx, st.bal); err != nil {
			return nil, err
		}
		for _, e := range st.extra {
			e.PolicyVersion = st.p.Version
			seq, err := tx.InsertLedgerEntry(ctx, e)
			if err != nil {
				return nil, err
			}
			e.Seq = seq
			allEntries = append(allEntries, entryToView(e))
		}
	}
	for _, it := range items {
		st := stagedMap[it.Resource]
		st.bal.Balance -= it.Amount
		entry := domain.LedgerEntry{
			TenantID:      tenantID,
			Type:          domain.EntryConsume,
			Resource:      it.Resource,
			Amount:        it.Amount,
			PolicyVersion: st.p.Version,
			CycleNumber:   st.bal.CurrentCycle,
			Ts:            now,
		}
		seq, err := tx.InsertLedgerEntry(ctx, entry)
		if err != nil {
			return nil, err
		}
		entry.Seq = seq
		allEntries = append(allEntries, entryToView(entry))
		if err := tx.UpsertBalance(ctx, st.bal); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, balanceToSnapshot(st.bal, st.p))
	}
	// Dedup snapshots by resource (items may repeat a resource after merge
	// they won't, but be safe).
	snapshots = dedupSnapshots(snapshots)
	return &DeductResponse{
		TenantID: tenantID,
		Balances: snapshots,
		Entries:  allEntries,
	}, nil
}

// Reserve creates a pending reservation, atomically freezing quota across all
// resource dimensions.
func (s *Service) Reserve(ctx context.Context, req ReserveRequest) (*ReserveResponse, error) {
	items, err := validateItems(req.Items)
	if err != nil {
		return nil, err
	}
	if req.TenantID == "" {
		return nil, NewError(CodeInvalidInput, "tenant id is required")
	}
	if req.ExpiresAt != 0 && req.ExpiresAt < int64(s.clock.Now()) {
		return nil, NewError(CodeInvalidInput, "expires_at is in the past")
	}
	if err := s.requireStarted(); err != nil {
		return nil, err
	}

	reservationID := s.ids.NewID()
	// Include the generated reservation ID in the idempotency hash so that a
	// replayed request (key hit) returns the same ID without re-generating.
	idempotencyReq := reserveIdempotencyRequest{
		Items:     items,
		ExpiresAt: req.ExpiresAt,
	}
	out, idempotent, err := idempotency(ctx, s, req.TenantID, req.IdempotencyKey, "reserve", idempotencyReq, func(tx store.Tx) (*ReserveResponse, error) {
		return s.reserveTx(ctx, tx, req.TenantID, reservationID, items, clock.Time(req.ExpiresAt))
	})
	if err != nil {
		return nil, err
	}
	resp := out
	resp.Idempotent = idempotent
	return resp, nil
}

type reserveIdempotencyRequest struct {
	Items     []domain.ReservationItem
	ExpiresAt int64
}

func (s *Service) reserveTx(ctx context.Context, tx store.Tx, tenantID, reservationID string, items []domain.ReservationItem, expiresAt clock.Time) (*ReserveResponse, error) {
	now := s.clock.Now()
	type staged struct {
		p     domain.Policy
		bal   domain.Balance
		extra []domain.LedgerEntry
	}
	stagedMap := make(map[string]staged, len(items))
	var sortedResources []string
	for _, it := range items {
		if _, ok := stagedMap[it.Resource]; ok {
			continue
		}
		p, err := tx.CurrentPolicy(ctx, tenantID, it.Resource)
		if err != nil {
			if store.IsNotFound(err) {
				return nil, NewError(CodeNotFound, "policy not found: "+it.Resource)
			}
			return nil, err
		}
		bal, err := tx.GetBalance(ctx, tenantID, it.Resource)
		if err != nil {
			if store.IsNotFound(err) {
				return nil, NewError(CodeInternal, "balance missing for policy")
			}
			return nil, err
		}
		bal, extra, err := settleTokenBucket(ctx, tx, p, bal, now)
		if err != nil {
			return nil, err
		}
		stagedMap[it.Resource] = staged{p: p, bal: bal, extra: extra}
		sortedResources = append(sortedResources, it.Resource)
	}
	sort.Strings(sortedResources)

	// Check availability across all dimensions.
	for _, it := range items {
		st := stagedMap[it.Resource]
		if st.bal.Available() < it.Amount {
			return nil, NewError(CodeInsufficientQuota, fmt.Sprintf("insufficient quota for %s: need %d have %d", it.Resource, it.Amount, st.bal.Available()))
		}
	}

	// Persist the lazy refill captured in the first pass. The balance was
	// already advanced by settleTokenBucket; here we write both the updated
	// projection and its matching refill ledger entry so the projection stays
	// equal to a fresh replay of the ledger. Re-settling would be a no-op
	// (settle is idempotent once last_refill_time has advanced) and would drop
	// the entry, so the entry must come from the first pass.
	var allEntries []LedgerEntryView
	for _, r := range sortedResources {
		st := stagedMap[r]
		if err := tx.UpsertBalance(ctx, st.bal); err != nil {
			return nil, err
		}
		for _, e := range st.extra {
			e.PolicyVersion = st.p.Version
			seq, err := tx.InsertLedgerEntry(ctx, e)
			if err != nil {
				return nil, err
			}
			e.Seq = seq
			allEntries = append(allEntries, entryToView(e))
		}
	}

	// Freeze quota and write reserve entries.
	var snapshots []BalanceSnapshot
	for _, it := range items {
		st := stagedMap[it.Resource]
		st.bal.Frozen += it.Amount
		entry := domain.LedgerEntry{
			TenantID:       tenantID,
			Type:           domain.EntryReserve,
			Resource:       it.Resource,
			Amount:         it.Amount,
			PolicyVersion:  st.p.Version,
			CycleNumber:    st.bal.CurrentCycle,
			ReservationID:  reservationID,
			Ts:             now,
		}
		seq, err := tx.InsertLedgerEntry(ctx, entry)
		if err != nil {
			return nil, err
		}
		entry.Seq = seq
		allEntries = append(allEntries, entryToView(entry))
		if err := tx.UpsertBalance(ctx, st.bal); err != nil {
			return nil, err
		}
		stagedMap[it.Resource] = st
	}
	for _, r := range sortedResources {
		st := stagedMap[r]
		snapshots = append(snapshots, balanceToSnapshot(st.bal, st.p))
	}

	resv := domain.Reservation{
		ID:        reservationID,
		TenantID:  tenantID,
		Status:    domain.StatusPending,
		Items:     items,
		CreatedAt: now,
		ExpiresAt: expiresAt,
	}
	if err := tx.InsertReservation(ctx, resv); err != nil {
		return nil, err
	}
	return &ReserveResponse{
		ReservationID: reservationID,
		TenantID:      tenantID,
		Status:        string(domain.StatusPending),
		Balances:      snapshots,
		Entries:       allEntries,
	}, nil
}

// Commit commits a pending reservation, charging the actual usage per resource
// and releasing the unused frozen amount.
func (s *Service) Commit(ctx context.Context, req CommitRequest) (*CommitResponse, error) {
	if req.ReservationID == "" || req.TenantID == "" {
		return nil, NewError(CodeInvalidInput, "reservation_id and tenant_id are required")
	}
	if err := s.requireStarted(); err != nil {
		return nil, err
	}
	// Validate usage values up front.
	for r, u := range req.Usage {
		if u < 0 {
			return nil, NewError(CodeInvalidInput, "usage must be non-negative: "+r)
		}
	}
	out, idempotent, err := idempotency(ctx, s, req.TenantID, req.IdempotencyKey, "commit", req, func(tx store.Tx) (*CommitResponse, error) {
		return s.commitTx(ctx, tx, req)
	})
	if err != nil {
		return nil, err
	}
	resp := out
	resp.Idempotent = idempotent
	return resp, nil
}

func (s *Service) commitTx(ctx context.Context, tx store.Tx, req CommitRequest) (*CommitResponse, error) {
	now := s.clock.Now()
	resv, err := tx.GetReservation(ctx, req.ReservationID)
	if err != nil {
		if store.IsNotFound(err) {
			return nil, NewError(CodeNotFound, "reservation not found")
		}
		return nil, err
	}
	if resv.TenantID != req.TenantID {
		return nil, NewError(CodeNotFound, "reservation not found")
	}
	if resv.Status != domain.StatusPending {
		return nil, NewError(CodeReservationTerminated, fmt.Sprintf("reservation is %s", resv.Status))
	}
	// Usage must not exceed frozen amount per resource.
	usage := make(map[string]int64, len(resv.Items))
	for _, it := range resv.Items {
		usage[it.Resource] = 0
	}
	for r, u := range req.Usage {
		if _, ok := usage[r]; !ok {
			return nil, NewError(CodeInvalidInput, "usage for non-reserved resource: "+r)
		}
		usage[r] = u
	}
	for _, it := range resv.Items {
		if usage[it.Resource] > it.Amount {
			return nil, NewError(CodeInvalidInput, fmt.Sprintf("usage exceeds reservation for %s: %d > %d", it.Resource, usage[it.Resource], it.Amount))
		}
	}

	var allEntries []LedgerEntryView
	var snapshots []BalanceSnapshot
	for _, it := range resv.Items {
		p, err := tx.CurrentPolicy(ctx, req.TenantID, it.Resource)
		if err != nil {
			return nil, err
		}
		bal, err := tx.GetBalance(ctx, req.TenantID, it.Resource)
		if err != nil {
			return nil, err
		}
		used := usage[it.Resource]
		// Charge actual usage.
		if used > 0 {
			bal.Balance -= used
			charge := domain.LedgerEntry{
				TenantID:       req.TenantID,
				Type:           domain.EntryCommitCharge,
				Resource:       it.Resource,
				Amount:         used,
				PolicyVersion:  p.Version,
				CycleNumber:    bal.CurrentCycle,
				ReservationID:  req.ReservationID,
				Ts:             now,
			}
			seq, err := tx.InsertLedgerEntry(ctx, charge)
			if err != nil {
				return nil, err
			}
			charge.Seq = seq
			allEntries = append(allEntries, entryToView(charge))
		}
		// Release the unused frozen portion.
		release := it.Amount - used
		bal.Frozen -= it.Amount
		if release > 0 {
			rel := domain.LedgerEntry{
				TenantID:       req.TenantID,
				Type:           domain.EntryReleaseUnused,
				Resource:       it.Resource,
				Amount:         release,
				PolicyVersion:  p.Version,
				CycleNumber:    bal.CurrentCycle,
				ReservationID:  req.ReservationID,
				Ts:             now,
			}
			seq, err := tx.InsertLedgerEntry(ctx, rel)
			if err != nil {
				return nil, err
			}
			rel.Seq = seq
			allEntries = append(allEntries, entryToView(rel))
		}
		if err := tx.UpsertBalance(ctx, bal); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, balanceToSnapshot(bal, p))
	}
	if err := tx.UpdateReservationStatus(ctx, req.ReservationID, domain.StatusCommitted); err != nil {
		return nil, err
	}
	return &CommitResponse{
		ReservationID: req.ReservationID,
		TenantID:      req.TenantID,
		Status:        string(domain.StatusCommitted),
		Balances:      snapshots,
		Entries:       allEntries,
	}, nil
}

// Rollback rolls back a pending reservation, releasing all frozen quota.
func (s *Service) Rollback(ctx context.Context, req RollbackRequest) (*RollbackResponse, error) {
	if req.ReservationID == "" || req.TenantID == "" {
		return nil, NewError(CodeInvalidInput, "reservation_id and tenant_id are required")
	}
	if err := s.requireStarted(); err != nil {
		return nil, err
	}
	out, idempotent, err := idempotency(ctx, s, req.TenantID, req.IdempotencyKey, "rollback", req, func(tx store.Tx) (*RollbackResponse, error) {
		return s.rollbackTx(ctx, tx, req)
	})
	if err != nil {
		return nil, err
	}
	resp := out
	resp.Idempotent = idempotent
	return resp, nil
}

func (s *Service) rollbackTx(ctx context.Context, tx store.Tx, req RollbackRequest) (*RollbackResponse, error) {
	now := s.clock.Now()
	resv, err := tx.GetReservation(ctx, req.ReservationID)
	if err != nil {
		if store.IsNotFound(err) {
			return nil, NewError(CodeNotFound, "reservation not found")
		}
		return nil, err
	}
	if resv.TenantID != req.TenantID {
		return nil, NewError(CodeNotFound, "reservation not found")
	}
	if resv.Status != domain.StatusPending {
		return nil, NewError(CodeReservationTerminated, fmt.Sprintf("reservation is %s", resv.Status))
	}

	var allEntries []LedgerEntryView
	var snapshots []BalanceSnapshot
	for _, it := range resv.Items {
		p, err := tx.CurrentPolicy(ctx, req.TenantID, it.Resource)
		if err != nil {
			return nil, err
		}
		bal, err := tx.GetBalance(ctx, req.TenantID, it.Resource)
		if err != nil {
			return nil, err
		}
		bal.Frozen -= it.Amount
		entry := domain.LedgerEntry{
			TenantID:       req.TenantID,
			Type:           domain.EntryRollbackRelease,
			Resource:       it.Resource,
			Amount:         it.Amount,
			PolicyVersion:  p.Version,
			CycleNumber:    bal.CurrentCycle,
			ReservationID:  req.ReservationID,
			Ts:             now,
		}
		seq, err := tx.InsertLedgerEntry(ctx, entry)
		if err != nil {
			return nil, err
		}
		entry.Seq = seq
		allEntries = append(allEntries, entryToView(entry))
		if err := tx.UpsertBalance(ctx, bal); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, balanceToSnapshot(bal, p))
	}
	if err := tx.UpdateReservationStatus(ctx, req.ReservationID, domain.StatusRolledBack); err != nil {
		return nil, err
	}
	return &RollbackResponse{
		ReservationID: req.ReservationID,
		TenantID:      req.TenantID,
		Status:        string(domain.StatusRolledBack),
		Balances:      snapshots,
		Entries:       allEntries,
	}, nil
}

// GetReservation returns a reservation by ID.
func (s *Service) GetReservation(ctx context.Context, tenantID, id string) (domain.Reservation, error) {
	var resv domain.Reservation
	err := s.store.View(ctx, func(tx store.Tx) error {
		r, err := tx.GetReservation(ctx, id)
		if err != nil {
			return err
		}
		if r.TenantID != tenantID {
			return NewError(CodeNotFound, "reservation not found")
		}
		resv = r
		return nil
	})
	return resv, err
}

func (s *Service) requireStarted() error {
	if s.faulted {
		return NewError(CodeReadonly, "service is in read-only fault state: "+s.faultReasonCache)
	}
	if !s.started {
		return NewError(CodeInternal, "service not started; call Recover first")
	}
	return nil
}

// Started reports whether Recover has completed successfully.
func (s *Service) Started() bool { return s.started }

func dedupSnapshots(in []BalanceSnapshot) []BalanceSnapshot {
	seen := make(map[string]int, len(in))
	out := in[:0]
	for _, s := range in {
		if idx, ok := seen[s.Resource]; ok {
			out[idx] = s
			continue
		}
		seen[s.Resource] = len(out)
		out = append(out, s)
	}
	return out
}
