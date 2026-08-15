package store

import (
	"context"
	"errors"
	"sync/atomic"
)

// FaultInjector wraps a Store and can be configured to fail transactions at
// controlled points. It is intended for tests that must verify atomicity:
// when a transaction fails after the callback has performed writes but before
// commit, the underlying SQLite transaction rolls back, leaving no
// half-written state.
//
// The injector is deterministic: the caller decides exactly when failures
// occur by toggling flags or counting updates, so tests do not depend on
// random scheduling.
type FaultInjector struct {
	inner Store

	// failCommit, when true, makes every subsequent Update return an error
	// after the callback succeeds but before the transaction commits. The
	// underlying transaction is rolled back by the store.
	failCommit atomic.Bool
	// failAfterN commits the first N updates normally and fails the (N+1)th
	// at the commit point. Zero means disabled.
	failAfterN atomic.Int64
	count      atomic.Int64

	// injectedErr is the sentinel error used by injected faults.
	injectedErr error
}

// NewFaultInjector wraps inner with fault-injection capability.
func NewFaultInjector(inner Store) *FaultInjector {
	return &FaultInjector{inner: inner, injectedErr: errors.New("quotaraft: injected fault")}
}

// InjectedError returns the sentinel error used by injected faults.
func (f *FaultInjector) InjectedError() error { return f.injectedErr }

// FailNextCommit configures the injector to fail the commit of every
// subsequent Update until cleared.
func (f *FaultInjector) FailNextCommit(yes bool) {
	f.failCommit.Store(yes)
}

// FailAfterNCommits commits the first n updates successfully, then fails the
// next one at commit time. n<=0 disables.
func (f *FaultInjector) FailAfterNCommits(n int64) {
	f.failAfterN.Store(n)
}

// Count returns the number of Update callbacks that have run.
func (f *FaultInjector) Count() int64 { return f.count.Load() }

// Update wraps the inner store's Update, injecting commit failures. Returning
// a non-nil error from the callback causes the transaction to roll back, so an
// injected fault produces a clean rollback with no persistent side effects.
func (f *FaultInjector) Update(ctx context.Context, fn func(Tx) error) error {
	return f.inner.Update(ctx, func(tx Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		// After the callback succeeds, decide whether to fail the commit.
		if f.failCommit.Load() {
			return f.injectedErr
		}
		if n := f.failAfterN.Load(); n > 0 {
			c := f.count.Add(1)
			if c > n {
				return f.injectedErr
			}
		}
		return nil
	})
}

// View delegates to the inner store.
func (f *FaultInjector) View(ctx context.Context, fn func(Tx) error) error {
	return f.inner.View(ctx, fn)
}

// Close delegates to the inner store.
func (f *FaultInjector) Close() error { return f.inner.Close() }

// Path delegates to the inner store.
func (f *FaultInjector) Path() string { return f.inner.Path() }

// Ensure FaultInjector satisfies Store.
var _ Store = (*FaultInjector)(nil)
