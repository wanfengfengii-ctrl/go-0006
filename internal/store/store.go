// Package store defines the persistence interface for QuotaRaft and a SQLite
// implementation backed by modernc.org/sqlite (a pure-Go driver, so the same
// binary runs on linux/amd64 and linux/arm64 without CGO).
//
// Concurrency model: QuotaRaft is a single-writer service. The SQLite store
// serializes all write transactions with a process-level mutex, which removes
// SQLITE_BUSY contention from in-process goroutines. A busy_timeout and a
// limited retry loop remain for safety against any external writer. Because
// each mutating operation runs entirely inside one BEGIN/COMMIT transaction,
// a failure at any point rolls back the whole operation: there is never a
// half-written projection, reservation, or ledger entry.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/quotaraft/quotaraft/internal/clock"
	"github.com/quotaraft/quotaraft/internal/domain"
)

// ErrNotFound is returned by read methods when no row matches.
var ErrNotFound = errors.New("quotaraft: not found")

// ErrBusy is returned when a transaction cannot acquire the write lock after
// the configured number of retries. In the single-writer deployment this
// should never happen; it is surfaced so callers can distinguish it from
// other failures.
var ErrBusy = errors.New("quotaraft: database busy")

// Tx is the transaction handle passed to Update callbacks. All methods execute
// against the current transaction; they are not safe to use after the
// callback returns.
type Tx interface {
	// Tenant operations.
	UpsertTenant(ctx context.Context, t domain.Tenant) error
	GetTenant(ctx context.Context, id string) (domain.Tenant, error)

	// Policy operations. CurrentPolicy returns the highest version.
	UpsertPolicy(ctx context.Context, p domain.Policy) error
	GetPolicy(ctx context.Context, tenantID, resource string, version int64) (domain.Policy, error)
	CurrentPolicy(ctx context.Context, tenantID, resource string) (domain.Policy, error)
	ListPolicies(ctx context.Context, tenantID string) ([]domain.Policy, error)

	// Balance projection.
	GetBalance(ctx context.Context, tenantID, resource string) (domain.Balance, error)
	UpsertBalance(ctx context.Context, b domain.Balance) error
	ListBalances(ctx context.Context, tenantID string) ([]domain.Balance, error)
	ListAllBalances(ctx context.Context) ([]domain.Balance, error)

	// Reservations.
	InsertReservation(ctx context.Context, r domain.Reservation) error
	GetReservation(ctx context.Context, id string) (domain.Reservation, error)
	ListPendingReservations(ctx context.Context, tenantID string) ([]domain.Reservation, error)
	ListAllPendingReservations(ctx context.Context) ([]domain.Reservation, error)
	UpdateReservationStatus(ctx context.Context, id string, status domain.Status) error

	// Ledger.
	InsertLedgerEntry(ctx context.Context, e domain.LedgerEntry) (int64, error)
	ListLedgerEntries(ctx context.Context, tenantID string, afterSeq int64, limit int) ([]domain.LedgerEntry, error)
	ListLedgerEntriesForResource(ctx context.Context, tenantID, resource string) ([]domain.LedgerEntry, error)
	ListAllLedgerEntries(ctx context.Context) ([]domain.LedgerEntry, error)

	// Idempotency.
	GetIdempotencyRecord(ctx context.Context, tenantID, key string) (IdempotencyRecord, error)
	InsertIdempotencyRecord(ctx context.Context, r IdempotencyRecord) error

	// Coordinator state.
	GetCoordinatorState(ctx context.Context) (domain.CoordinatorState, error)
	SetCoordinatorState(ctx context.Context, s domain.CoordinatorState) error
}

// IdempotencyRecord stores a request digest and the full response so that a
// retried request with the same key returns the original result.
type IdempotencyRecord struct {
	TenantID    string
	Key         string
	RequestHash string
	Response    []byte // serialized JSON response
	CreatedAt   clock.Time
}

// Store is the persistence boundary. Update runs fn inside a serialized
// transaction; if fn returns an error the transaction is rolled back.
type Store interface {
	// Update executes fn within a single write transaction. The transaction
	// is serialized with all other writes. Any error from fn or from the
	// commit causes a full rollback.
	Update(ctx context.Context, fn func(Tx) error) error

	// View executes fn within a read transaction.
	View(ctx context.Context, fn func(Tx) error) error

	// Close releases the underlying database resources.
	Close() error

	// Path returns the database file path (mainly for diagnostics).
	Path() string
}

// IsNotFound reports whether err is ErrNotFound.
func IsNotFound(err error) bool { return errors.Is(err, ErrNotFound) }

// IsBusy reports whether err is ErrBusy.
func IsBusy(err error) bool { return errors.Is(err, ErrBusy) }

// faultPoint identifies a point in a transaction where a fault can be injected
// for testing. It is exported so the fault-injecting wrapper can reference it.
type faultPoint int

const (
	faultBeforeUpdate faultPoint = iota
	faultAfterUpdateBeforeCommit
)

// String returns a human-readable name for diagnostics.
func (p faultPoint) String() string {
	switch p {
	case faultBeforeUpdate:
		return "before_update"
	case faultAfterUpdateBeforeCommit:
		return "after_update_before_commit"
	default:
		return fmt.Sprintf("fault_point(%d)", int(p))
	}
}
