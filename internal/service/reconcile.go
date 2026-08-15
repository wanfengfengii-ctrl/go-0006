package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/quotaraft/quotaraft/internal/domain"
	"github.com/quotaraft/quotaraft/internal/store"
)

// Reconcile rebuilds every balance projection from the immutable ledger and
// compares it to the online projection. It returns the mismatches and, if any
// are found, optionally marks the service faulted (read-only).
//
// The rebuild starts from zero and applies every entry's BalanceDelta and
// FrozenDelta in sequence order. Because the opening balance of each policy is
// itself recorded as a refill or cycle_reset entry, the rebuild is fully
// self-contained: it needs no out-of-band starting state.
func (s *Service) Reconcile(ctx context.Context) (ReconcileResult, error) {
	var online []domain.Balance
	var allEntries []domain.LedgerEntry
	err := s.store.View(ctx, func(tx store.Tx) error {
		b, err := tx.ListAllBalances(ctx)
		if err != nil {
			return err
		}
		online = b
		e, err := tx.ListAllLedgerEntries(ctx)
		if err != nil {
			return err
		}
		allEntries = e
		return nil
	})
	if err != nil {
		return ReconcileResult{}, err
	}

	// Group entries by (tenant, resource) and accumulate deltas in seq order.
	type key struct{ tenant, resource string }
	type acc struct{ balance, frozen int64 }
	rebuilt := make(map[key]acc)
	keys := make([]key, 0)
	for _, e := range allEntries {
		k := key{e.TenantID, e.Resource}
		if _, ok := rebuilt[k]; !ok {
			keys = append(keys, k)
		}
		a := rebuilt[k]
		a.balance += e.BalanceDelta()
		a.frozen += e.FrozenDelta()
		rebuilt[k] = a
	}

	result := ReconcileResult{OK: true}
	onlineMap := make(map[key]domain.Balance)
	for _, b := range online {
		onlineMap[key{b.TenantID, b.Resource}] = b
	}

	// Compare rebuilt vs online for every key present in either.
	seen := make(map[key]bool)
	for _, b := range online {
		seen[key{b.TenantID, b.Resource}] = true
	}
	for _, k := range keys {
		seen[k] = true
	}
	var allKeys []key
	for k := range seen {
		allKeys = append(allKeys, k)
	}
	sort.Slice(allKeys, func(i, j int) bool {
		if allKeys[i].tenant != allKeys[j].tenant {
			return allKeys[i].tenant < allKeys[j].tenant
		}
		return allKeys[i].resource < allKeys[j].resource
	})

	for _, k := range allKeys {
		r := rebuilt[k]
		o, ok := onlineMap[k]
		if !ok {
			// Online projection missing but ledger has entries.
			result.OK = false
			result.Mismatches = append(result.Mismatches, Mismatch{
				TenantID: k.tenant, Resource: k.resource,
				Field:   "balance",
				Online:  0,
				Rebuilt: r.balance,
			})
			continue
		}
		if o.Balance != r.balance {
			result.OK = false
			result.Mismatches = append(result.Mismatches, Mismatch{
				TenantID: k.tenant, Resource: k.resource,
				Field:  "balance",
				Online: o.Balance,
				Rebuilt: r.balance,
			})
		}
		if o.Frozen != r.frozen {
			result.OK = false
			result.Mismatches = append(result.Mismatches, Mismatch{
				TenantID: k.tenant, Resource: k.resource,
				Field:  "frozen",
				Online: o.Frozen,
				Rebuilt: r.frozen,
			})
		}
	}
	return result, nil
}

// RebuildProjections recomputes every balance projection from the ledger and
// persists the rebuilt values. It is an administrative repair operation.
// Unlike Reconcile (which only reports), it overwrites the online projection.
func (s *Service) RebuildProjections(ctx context.Context) (ReconcileResult, error) {
	res, err := s.Reconcile(ctx)
	if err != nil {
		return res, err
	}
	// Gather rebuilt values and persist them.
	type key struct{ tenant, resource string }
	type acc struct {
		balance, frozen int64
		policyVersion   int64
	}
	rebuilt := make(map[key]acc)
	var entries []domain.LedgerEntry
	var existing []domain.Balance
	if err := s.store.View(ctx, func(tx store.Tx) error {
		var err error
		entries, err = tx.ListAllLedgerEntries(ctx)
		if err != nil {
			return err
		}
		existing, err = tx.ListAllBalances(ctx)
		return err
	}); err != nil {
		return res, err
	}
	for _, e := range entries {
		k := key{e.TenantID, e.Resource}
		a := rebuilt[k]
		a.balance += e.BalanceDelta()
		a.frozen += e.FrozenDelta()
		a.policyVersion = e.PolicyVersion // latest wins (entries are in seq order)
		rebuilt[k] = a
	}
	// Preserve non-balance projection fields (last_refill_time, current_cycle)
	// from the existing online projection; only balance/frozen/policy_version
	// are rebuilt from the ledger.
	existMap := make(map[key]domain.Balance)
	for _, b := range existing {
		existMap[key{b.TenantID, b.Resource}] = b
	}

	err = s.store.Update(ctx, func(tx store.Tx) error {
		for k, a := range rebuilt {
			bal := domain.Balance{
				TenantID:       k.tenant,
				Resource:       k.resource,
				PolicyVersion:  a.policyVersion,
				Balance:        a.balance,
				Frozen:         a.frozen,
				LastRefillTime: existMap[k].LastRefillTime,
				CurrentCycle:   existMap[k].CurrentCycle,
			}
			if err := tx.UpsertBalance(ctx, bal); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return res, err
	}
	// Re-check after rebuild.
	return s.Reconcile(ctx)
}

// Recover is called at startup. It verifies the projection against the ledger
// and, if consistent, runs one maintenance pass at the current injected time
// to settle expired reservations and crossed cycles. If the projection is
// inconsistent the service enters a read-only fault state: queries and
// diagnostics remain available, but mutations are rejected with CodeReadonly.
func (s *Service) Recover(ctx context.Context) error {
	res, err := s.Reconcile(ctx)
	if err != nil {
		return s.fault(ctx, fmt.Sprintf("reconcile failed: %v", err))
	}
	if !res.OK {
		return s.fault(ctx, summarize(res))
	}
	// Run a maintenance pass to settle expiries and crossed cycles as of now.
	_, err = s.RunMaintenanceForRecovery(ctx)
	if err != nil {
		return s.fault(ctx, fmt.Sprintf("recovery maintenance failed: %v", err))
	}
	// Re-verify after maintenance.
	res, err = s.Reconcile(ctx)
	if err != nil {
		return s.fault(ctx, fmt.Sprintf("post-maintenance reconcile failed: %v", err))
	}
	if !res.OK {
		return s.fault(ctx, summarize(res))
	}
	s.started = true
	s.faulted = false
	s.faultReasonCache = ""
	return nil
}

// RunMaintenanceForRecovery runs maintenance but does not require the service
// to be started; it is used during Recover before the service is opened.
func (s *Service) RunMaintenanceForRecovery(ctx context.Context) (MaintenanceResult, error) {
	now := s.clock.Now()
	var res MaintenanceResult
	err := s.store.Update(ctx, func(tx store.Tx) error {
		return s.maintenanceTx(ctx, tx, now, &res)
	})
	return res, err
}

// Faulted reports whether the service is in a read-only fault state.
func (s *Service) Faulted() bool {
	return s.faulted
}

// FaultReason returns a human-readable description of the current fault, if any.
func (s *Service) FaultReason() string {
	return s.faultReasonCache
}

// fault marks the service as faulted (read-only) and persists the reason.
func (s *Service) fault(ctx context.Context, reason string) error {
	s.started = false
	s.faulted = true
	s.faultReasonCache = reason
	return s.store.Update(ctx, func(tx store.Tx) error {
		state, err := tx.GetCoordinatorState(ctx)
		if err != nil {
			return err
		}
		state.Faulted = true
		state.FaultReason = reason
		return tx.SetCoordinatorState(ctx, state)
	})
}

// ClearFault rebuilds projections from the ledger and, if consistent, clears
// the fault state so the service can accept mutations again.
func (s *Service) ClearFault(ctx context.Context) error {
	if _, err := s.RebuildProjections(ctx); err != nil {
		return err
	}
	res, err := s.Reconcile(ctx)
	if err != nil {
		return err
	}
	if !res.OK {
		return NewError(CodeInternal, "rebuild did not resolve mismatch: " + summarize(res))
	}
	s.faulted = false
	s.faultReasonCache = ""
	s.started = true
	return s.store.Update(ctx, func(tx store.Tx) error {
		state, err := tx.GetCoordinatorState(ctx)
		if err != nil {
			return err
		}
		state.Faulted = false
		state.FaultReason = ""
		return tx.SetCoordinatorState(ctx, state)
	})
}

func summarize(res ReconcileResult) string {
	if len(res.Mismatches) == 0 {
		return "unknown mismatch"
	}
	m := res.Mismatches[0]
	return fmt.Sprintf("projection mismatch: tenant=%s resource=%s field=%s online=%d rebuilt=%d",
		m.TenantID, m.Resource, m.Field, m.Online, m.Rebuilt)
}
