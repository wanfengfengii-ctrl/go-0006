package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/quotaraft/quotaraft/internal/clock"
	"github.com/quotaraft/quotaraft/internal/domain"
	"github.com/quotaraft/quotaraft/internal/engine"
	"github.com/quotaraft/quotaraft/internal/store"
)

// RunMaintenance performs one deterministic maintenance pass at the current
// injected time. Within a single transaction it:
//  1. Expires pending reservations whose deadline has passed (expiry_release),
//     processed in stable (tenant, reservation) order.
//  2. Materializes token-bucket refills that are due (refill), in stable
//     (tenant, resource) order.
//  3. Advances cycle policies across crossed cycle boundaries (cycle_reset),
//     in stable (tenant, resource) order.
//
// The pass is idempotent: re-running it at the same logical time produces no
// new entries, because settled refills advance last_refill_time, expired
// reservations are no longer pending, and cycle projections advance
// current_cycle.
func (s *Service) RunMaintenance(ctx context.Context) (MaintenanceResult, error) {
	if err := s.requireStarted(); err != nil {
		return MaintenanceResult{}, err
	}
	now := s.clock.Now()
	var res MaintenanceResult
	err := s.store.Update(ctx, func(tx store.Tx) error {
		return s.maintenanceTx(ctx, tx, now, &res)
	})
	if err != nil {
		return MaintenanceResult{}, err
	}
	return res, nil
}

// maintenanceTx performs the maintenance work within an open transaction. It
// is shared by RunMaintenance (post-start) and RunMaintenanceForRecovery
// (during Recover, before start). The result is accumulated into res.
func (s *Service) maintenanceTx(ctx context.Context, tx store.Tx, now clock.Time, res *MaintenanceResult) error {
	var entries []domain.LedgerEntry

	err := (func() error {
		// --- 1. Expiry ---
		pending, err := tx.ListAllPendingReservations(ctx)
		if err != nil {
			return err
		}
		// Stable order by (tenant, reservation id).
		sort.Slice(pending, func(i, j int) bool {
			if pending[i].TenantID != pending[j].TenantID {
				return pending[i].TenantID < pending[j].TenantID
			}
			return pending[i].ID < pending[j].ID
		})
		for _, r := range pending {
			if !r.IsExpired(now) {
				continue
			}
			// Items are already sorted by resource from scanReservations.
			for _, it := range r.Items {
				p, err := tx.CurrentPolicy(ctx, r.TenantID, it.Resource)
				if err != nil {
					return err
				}
				bal, err := tx.GetBalance(ctx, r.TenantID, it.Resource)
				if err != nil {
					return err
				}
				bal.Frozen -= it.Amount
				entry := domain.LedgerEntry{
					TenantID:       r.TenantID,
					Type:           domain.EntryExpiryRelease,
					Resource:       it.Resource,
					Amount:         it.Amount,
					PolicyVersion:  p.Version,
					CycleNumber:    bal.CurrentCycle,
					ReservationID:  r.ID,
					Ts:             now,
				}
				seq, err := tx.InsertLedgerEntry(ctx, entry)
				if err != nil {
					return err
				}
				entry.Seq = seq
				entries = append(entries, entry)
				if err := tx.UpsertBalance(ctx, bal); err != nil {
					return err
				}
			}
			if err := tx.UpdateReservationStatus(ctx, r.ID, domain.StatusExpired); err != nil {
				return err
			}
			res.ExpiredCount++
		}

		// --- 2. Token-bucket refills ---
		bals, err := tx.ListAllBalances(ctx)
		if err != nil {
			return err
		}
		// Filter to token-bucket policies, stable by (tenant, resource).
		var tb []struct {
			bal domain.Balance
			p   domain.Policy
		}
		for _, bal := range bals {
			p, err := tx.CurrentPolicy(ctx, bal.TenantID, bal.Resource)
			if err != nil {
				return err
			}
			if p.Type == domain.PolicyTokenBucket {
				tb = append(tb, struct {
					bal domain.Balance
					p   domain.Policy
				}{bal, p})
			}
		}
		sort.Slice(tb, func(i, j int) bool {
			if tb[i].bal.TenantID != tb[j].bal.TenantID {
				return tb[i].bal.TenantID < tb[j].bal.TenantID
			}
			return tb[i].bal.Resource < tb[j].bal.Resource
		})
		for _, e := range tb {
			bal, extra, err := settleTokenBucket(ctx, tx, e.p, e.bal, now)
			if err != nil {
				return err
			}
			if len(extra) == 0 {
				// Still persist if last_refill_time advanced (even with no
				// tokens added, e.g. bucket at capacity).
				if bal.LastRefillTime != e.bal.LastRefillTime {
					if err := tx.UpsertBalance(ctx, bal); err != nil {
						return err
					}
				}
				continue
			}
			for _, en := range extra {
				en.PolicyVersion = e.p.Version
				seq, err := tx.InsertLedgerEntry(ctx, en)
				if err != nil {
					return err
				}
				en.Seq = seq
				entries = append(entries, en)
				res.RefillCount++
			}
			if err := tx.UpsertBalance(ctx, bal); err != nil {
				return err
			}
		}

		// --- 3. Cycle resets ---
		bals, err = tx.ListAllBalances(ctx)
		if err != nil {
			return err
		}
		var cyc []struct {
			bal domain.Balance
			p   domain.Policy
		}
		for _, bal := range bals {
			p, err := tx.CurrentPolicy(ctx, bal.TenantID, bal.Resource)
			if err != nil {
				return err
			}
			if p.Type == domain.PolicyCycle {
				cyc = append(cyc, struct {
					bal domain.Balance
					p   domain.Policy
				}{bal, p})
			}
		}
		sort.Slice(cyc, func(i, j int) bool {
			if cyc[i].bal.TenantID != cyc[j].bal.TenantID {
				return cyc[i].bal.TenantID < cyc[j].bal.TenantID
			}
			return cyc[i].bal.Resource < cyc[j].bal.Resource
		})
		for _, e := range cyc {
			target, err := engine.CycleNumber(e.p.CycleAnchor, e.p.CycleLength, now)
			if err != nil {
				return err
			}
			for e.bal.CurrentCycle < target {
				// Reset balance to limit at this boundary, if consumed.
				amount := engine.CycleResetAmount(e.p.CycleLimit, e.bal.Balance)
				if amount > 0 {
					entry := domain.LedgerEntry{
						TenantID:      e.bal.TenantID,
						Type:          domain.EntryCycleReset,
						Resource:      e.bal.Resource,
						Amount:        amount,
						PolicyVersion: e.p.Version,
						CycleNumber:   e.bal.CurrentCycle,
						Ts:            now,
					}
					seq, err := tx.InsertLedgerEntry(ctx, entry)
					if err != nil {
						return err
					}
					entry.Seq = seq
					entries = append(entries, entry)
					e.bal.Balance = e.p.CycleLimit
					res.CycleResetCount++
				}
				e.bal.CurrentCycle++
			}
			if e.bal.CurrentCycle != e.bal.PolicyVersion {
				// current_cycle may have advanced; persist regardless.
			}
			if err := tx.UpsertBalance(ctx, e.bal); err != nil {
				return err
			}
		}

		// Persist water mark.
		state, err := tx.GetCoordinatorState(ctx)
		if err != nil {
			return err
		}
		state.LastMaintenance = now
		if err := tx.SetCoordinatorState(ctx, state); err != nil {
			return err
		}
		return nil
	})()
	for _, e := range entries {
		res.Entries = append(res.Entries, entryToView(e))
	}
	return err
}

// Drain runs maintenance repeatedly until a full pass produces no entries,
// which advances the clock-driven state to a fixed point. It is intended for
// tests that want to settle everything without specifying exact times.
func (s *Service) Drain(ctx context.Context) (MaintenanceResult, error) {
	var total MaintenanceResult
	for i := 0; i < 100; i++ {
		r, err := s.RunMaintenance(ctx)
		if err != nil {
			return total, err
		}
		total.ExpiredCount += r.ExpiredCount
		total.RefillCount += r.RefillCount
		total.CycleResetCount += r.CycleResetCount
		total.Entries = append(total.Entries, r.Entries...)
		if r.ExpiredCount == 0 && r.RefillCount == 0 && r.CycleResetCount == 0 {
			return total, nil
		}
	}
	return total, fmt.Errorf("drain did not converge")
}

// AdvanceTime is a convenience for tests using a manual clock: it advances
// the clock and runs maintenance once. It requires the service clock to be a
// *clock.Manual.
func (s *Service) AdvanceTime(ctx context.Context, d clock.Duration) (MaintenanceResult, error) {
	m, ok := s.clock.(*clock.Manual)
	if !ok {
		return MaintenanceResult{}, NewError(CodeInvalidInput, "advance-time requires a manual clock")
	}
	m.Advance(d)
	return s.RunMaintenance(ctx)
}
