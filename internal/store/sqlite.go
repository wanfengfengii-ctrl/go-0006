package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"modernc.org/sqlite"

	"github.com/quotaraft/quotaraft/internal/clock"
	"github.com/quotaraft/quotaraft/internal/domain"
	"github.com/quotaraft/quotaraft/internal/migrations"
)

const (
	driverName      = "sqlite"
	defaultBusyMS   = 5000
	defaultRetryMax = 8
)

// SQLiteStore is a single-writer Store backed by SQLite (pure-Go driver).
type SQLiteStore struct {
	db       *sql.DB
	path     string
	writeMu  sync.Mutex
	retryMax int
}

// Options configures store creation.
type Options struct {
	// Path is the database file path. Use ":memory:" for an ephemeral DB.
	Path string
	// RetryMax is the maximum number of busy retries per transaction.
	RetryMax int
	// BusyTimeoutMs is the SQLite busy_timeout pragma value.
	BusyTimeoutMs int
}

// New opens or creates a SQLite database, applies the schema, and returns a
// ready Store. The connection pool is configured for a single writer.
func New(opts Options) (*SQLiteStore, error) {
	if opts.Path == "" {
		opts.Path = ":memory:"
	}
	if opts.RetryMax <= 0 {
		opts.RetryMax = defaultRetryMax
	}
	if opts.BusyTimeoutMs <= 0 {
		opts.BusyTimeoutMs = defaultBusyMS
	}

	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(%d)&_pragma=foreign_keys(on)&_pragma=journal_mode(WAL)", opts.Path, opts.BusyTimeoutMs)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// Single writer: keep the pool small and set a generous connection lifetime.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	s := &SQLiteStore{db: db, path: opts.Path, retryMax: opts.RetryMax}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *SQLiteStore) migrate(ctx context.Context) error {
	for _, stmt := range migrations.Statements {
		stmt := trimStmt(stmt)
		if stmt == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec migration statement: %w", err)
		}
	}
	// Record schema version idempotently.
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO schema_version(version) VALUES(?)`, migrations.CurrentVersion)
	return err
}

// Path returns the database file path.
func (s *SQLiteStore) Path() string { return s.path }

// Close closes the database.
func (s *SQLiteStore) Close() error { return s.db.Close() }

// Update runs fn in a serialized write transaction.
func (s *SQLiteStore) Update(ctx context.Context, fn func(Tx) error) (err error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.runTx(ctx, false, fn)
}

// View runs fn in a read transaction.
func (s *SQLiteStore) View(ctx context.Context, fn func(Tx) error) (err error) {
	// Views do not need the write mutex, but using a read transaction keeps
	// them consistent. To keep things simple and avoid pool races with the
	// single connection, we still acquire the write mutex for the duration.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.runTx(ctx, true, fn)
}

func (s *SQLiteStore) runTx(ctx context.Context, readOnly bool, fn func(Tx) error) error {
	var lastErr error
	for attempt := 0; attempt <= s.retryMax; attempt++ {
		err := s.attemptTx(ctx, fn)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isBusyErr(err) {
			return err
		}
		// Backoff before retrying on busy.
		if attempt < s.retryMax {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(1<<attempt) * time.Millisecond):
			}
		}
	}
	return fmt.Errorf("%w: %v", ErrBusy, lastErr)
}

func (s *SQLiteStore) attemptTx(ctx context.Context, fn func(Tx) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	t := &sqliteTx{tx: tx}
	if err = fn(t); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func isBusyErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrBusy) {
		return true
	}
	var se *sqlite.Error
	if errors.As(err, &se) {
		// SQLITE_BUSY (5) and SQLITE_BUSY_SNAPSHOT, etc., share code 5.
		code := se.Code()
		if code == 5 || code == 6 { // busy / locked
			return true
		}
	}
	return false
}

// sqliteTx implements Tx against a *sql.Tx.
type sqliteTx struct {
	tx *sql.Tx
}

func (t *sqliteTx) UpsertTenant(ctx context.Context, tn domain.Tenant) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO tenants(id, created_at) VALUES(?, ?)
		 ON CONFLICT(id) DO NOTHING`, tn.ID, int64(tn.CreatedAt))
	return err
}

func (t *sqliteTx) GetTenant(ctx context.Context, id string) (domain.Tenant, error) {
	var tn domain.Tenant
	var created int64
	err := t.tx.QueryRowContext(ctx, `SELECT id, created_at FROM tenants WHERE id=?`, id).
		Scan(&tn.ID, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return tn, ErrNotFound
	}
	if err != nil {
		return tn, err
	}
	tn.CreatedAt = clock.Time(created)
	return tn, nil
}

func (t *sqliteTx) UpsertPolicy(ctx context.Context, p domain.Policy) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO policies(tenant_id, resource, version, type, capacity, initial_tokens, refill_amount, refill_interval, cycle_limit, cycle_length, cycle_anchor, created_at)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(tenant_id, resource, version) DO NOTHING`,
		p.TenantID, p.Resource, p.Version, string(p.Type),
		p.Capacity, p.InitialTokens, p.RefillAmount, int64(p.RefillInterval),
		p.CycleLimit, int64(p.CycleLength), int64(p.CycleAnchor), int64(p.CreatedAt))
	return err
}

func (t *sqliteTx) GetPolicy(ctx context.Context, tenantID, resource string, version int64) (domain.Policy, error) {
	row := t.tx.QueryRowContext(ctx, policySelect+` WHERE tenant_id=? AND resource=? AND version=?`, tenantID, resource, version)
	return scanPolicy(row.Scan)
}

func (t *sqliteTx) CurrentPolicy(ctx context.Context, tenantID, resource string) (domain.Policy, error) {
	row := t.tx.QueryRowContext(ctx, policySelect+` WHERE tenant_id=? AND resource=? ORDER BY version DESC LIMIT 1`, tenantID, resource)
	return scanPolicy(row.Scan)
}

func (t *sqliteTx) ListPolicies(ctx context.Context, tenantID string) ([]domain.Policy, error) {
	rows, err := t.tx.QueryContext(ctx, policySelect+` WHERE tenant_id=? ORDER BY resource, version`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Policy
	for rows.Next() {
		p, err := scanPolicy(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

const policySelect = `SELECT tenant_id, resource, version, type, capacity, initial_tokens, refill_amount, refill_interval, cycle_limit, cycle_length, cycle_anchor, created_at FROM policies`

func scanPolicy(scan func(...any) error) (domain.Policy, error) {
	var p domain.Policy
	var typ string
	var refillInt, cycleLen, cycleAnc, created int64
	err := scan(&p.TenantID, &p.Resource, &p.Version, &typ,
		&p.Capacity, &p.InitialTokens, &p.RefillAmount, &refillInt,
		&p.CycleLimit, &cycleLen, &cycleAnc, &created)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return p, ErrNotFound
		}
		return p, err
	}
	p.Type = domain.PolicyType(typ)
	p.RefillInterval = clock.Duration(refillInt)
	p.CycleLength = clock.Duration(cycleLen)
	p.CycleAnchor = clock.Time(cycleAnc)
	p.CreatedAt = clock.Time(created)
	return p, nil
}

func (t *sqliteTx) GetBalance(ctx context.Context, tenantID, resource string) (domain.Balance, error) {
	var b domain.Balance
	var lastRefill int64
	err := t.tx.QueryRowContext(ctx,
		`SELECT tenant_id, resource, policy_version, balance, frozen, last_refill_time, current_cycle FROM balances WHERE tenant_id=? AND resource=?`,
		tenantID, resource).
		Scan(&b.TenantID, &b.Resource, &b.PolicyVersion, &b.Balance, &b.Frozen, &lastRefill, &b.CurrentCycle)
	if errors.Is(err, sql.ErrNoRows) {
		return b, ErrNotFound
	}
	if err != nil {
		return b, err
	}
	b.LastRefillTime = clock.Time(lastRefill)
	return b, nil
}

func (t *sqliteTx) UpsertBalance(ctx context.Context, b domain.Balance) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO balances(tenant_id, resource, policy_version, balance, frozen, last_refill_time, current_cycle)
		 VALUES(?,?,?,?,?,?,?)
		 ON CONFLICT(tenant_id, resource) DO UPDATE SET
		   policy_version=excluded.policy_version,
		   balance=excluded.balance,
		   frozen=excluded.frozen,
		   last_refill_time=excluded.last_refill_time,
		   current_cycle=excluded.current_cycle`,
		b.TenantID, b.Resource, b.PolicyVersion, b.Balance, b.Frozen, int64(b.LastRefillTime), b.CurrentCycle)
	return err
}

func (t *sqliteTx) ListBalances(ctx context.Context, tenantID string) ([]domain.Balance, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT tenant_id, resource, policy_version, balance, frozen, last_refill_time, current_cycle FROM balances WHERE tenant_id=? ORDER BY resource`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanBalances(rows)
}

func (t *sqliteTx) ListAllBalances(ctx context.Context) ([]domain.Balance, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT tenant_id, resource, policy_version, balance, frozen, last_refill_time, current_cycle FROM balances ORDER BY tenant_id, resource`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanBalances(rows)
}

func scanBalances(rows *sql.Rows) ([]domain.Balance, error) {
	var out []domain.Balance
	for rows.Next() {
		var b domain.Balance
		var lastRefill int64
		if err := rows.Scan(&b.TenantID, &b.Resource, &b.PolicyVersion, &b.Balance, &b.Frozen, &lastRefill, &b.CurrentCycle); err != nil {
			return nil, err
		}
		b.LastRefillTime = clock.Time(lastRefill)
		out = append(out, b)
	}
	return out, rows.Err()
}

func (t *sqliteTx) InsertReservation(ctx context.Context, r domain.Reservation) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO reservations(id, tenant_id, status, created_at, expires_at) VALUES(?,?,?,?,?)`,
		r.ID, r.TenantID, string(r.Status), int64(r.CreatedAt), int64(r.ExpiresAt))
	if err != nil {
		return err
	}
	for _, it := range r.Items {
		if _, err := t.tx.ExecContext(ctx,
			`INSERT INTO reservation_items(reservation_id, resource, amount) VALUES(?,?,?)`,
			r.ID, it.Resource, it.Amount); err != nil {
			return err
		}
	}
	return nil
}

func (t *sqliteTx) GetReservation(ctx context.Context, id string) (domain.Reservation, error) {
	var r domain.Reservation
	var status string
	var created, expires int64
	err := t.tx.QueryRowContext(ctx,
		`SELECT id, tenant_id, status, created_at, expires_at FROM reservations WHERE id=?`, id).
		Scan(&r.ID, &r.TenantID, &status, &created, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	if err != nil {
		return r, err
	}
	r.Status = domain.Status(status)
	r.CreatedAt = clock.Time(created)
	r.ExpiresAt = clock.Time(expires)

	rows, err := t.tx.QueryContext(ctx,
		`SELECT resource, amount FROM reservation_items WHERE reservation_id=? ORDER BY resource`, id)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		var it domain.ReservationItem
		if err := rows.Scan(&it.Resource, &it.Amount); err != nil {
			return r, err
		}
		r.Items = append(r.Items, it)
	}
	return r, rows.Err()
}

func (t *sqliteTx) ListPendingReservations(ctx context.Context, tenantID string) ([]domain.Reservation, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT id, tenant_id, status, created_at, expires_at FROM reservations WHERE tenant_id=? AND status='pending' ORDER BY id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanReservations(ctx, t, rows)
}

func (t *sqliteTx) ListAllPendingReservations(ctx context.Context) ([]domain.Reservation, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT id, tenant_id, status, created_at, expires_at FROM reservations WHERE status='pending' ORDER BY tenant_id, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanReservations(ctx, t, rows)
}

func scanReservations(ctx context.Context, t *sqliteTx, rows *sql.Rows) ([]domain.Reservation, error) {
	var out []domain.Reservation
	for rows.Next() {
		var r domain.Reservation
		var status string
		var created, expires int64
		if err := rows.Scan(&r.ID, &r.TenantID, &status, &created, &expires); err != nil {
			return nil, err
		}
		r.Status = domain.Status(status)
		r.CreatedAt = clock.Time(created)
		r.ExpiresAt = clock.Time(expires)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Load items for each reservation.
	for i := range out {
		itemRows, err := t.tx.QueryContext(ctx,
			`SELECT resource, amount FROM reservation_items WHERE reservation_id=? ORDER BY resource`, out[i].ID)
		if err != nil {
			return nil, err
		}
		for itemRows.Next() {
			var it domain.ReservationItem
			if err := itemRows.Scan(&it.Resource, &it.Amount); err != nil {
				_ = itemRows.Close()
				return nil, err
			}
			out[i].Items = append(out[i].Items, it)
		}
		_ = itemRows.Close()
	}
	return out, nil
}

func (t *sqliteTx) UpdateReservationStatus(ctx context.Context, id string, status domain.Status) error {
	res, err := t.tx.ExecContext(ctx,
		`UPDATE reservations SET status=? WHERE id=?`, string(status), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (t *sqliteTx) InsertLedgerEntry(ctx context.Context, e domain.LedgerEntry) (int64, error) {
	var resID any
	if e.ReservationID != "" {
		resID = e.ReservationID
	}
	var idem any
	if e.IdempotencyKey != "" {
		idem = e.IdempotencyKey
	}
	var seq int64
	err := t.tx.QueryRowContext(ctx,
		`INSERT INTO ledger_entries(tenant_id, type, resource, amount, policy_version, cycle_number, reservation_id, idempotency_key, ts)
		 VALUES(?,?,?,?,?,?,?,?,?)
		 RETURNING seq`,
		e.TenantID, string(e.Type), e.Resource, e.Amount, e.PolicyVersion, e.CycleNumber,
		resID, idem, int64(e.Ts)).Scan(&seq)
	if err != nil {
		return 0, err
	}
	return seq, nil
}

func (t *sqliteTx) ListLedgerEntries(ctx context.Context, tenantID string, afterSeq int64, limit int) ([]domain.LedgerEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := t.tx.QueryContext(ctx,
		`SELECT seq, tenant_id, type, resource, amount, policy_version, cycle_number, reservation_id, idempotency_key, ts
		 FROM ledger_entries WHERE tenant_id=? AND seq > ? ORDER BY seq ASC LIMIT ?`,
		tenantID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLedger(rows)
}

func (t *sqliteTx) ListLedgerEntriesForResource(ctx context.Context, tenantID, resource string) ([]domain.LedgerEntry, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT seq, tenant_id, type, resource, amount, policy_version, cycle_number, reservation_id, idempotency_key, ts
		 FROM ledger_entries WHERE tenant_id=? AND resource=? ORDER BY seq ASC`,
		tenantID, resource)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLedger(rows)
}

func (t *sqliteTx) ListAllLedgerEntries(ctx context.Context) ([]domain.LedgerEntry, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT seq, tenant_id, type, resource, amount, policy_version, cycle_number, reservation_id, idempotency_key, ts
		 FROM ledger_entries ORDER BY seq ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLedger(rows)
}

func scanLedger(rows *sql.Rows) ([]domain.LedgerEntry, error) {
	var out []domain.LedgerEntry
	for rows.Next() {
		var e domain.LedgerEntry
		var typ string
		var ts int64
		var resID, idem sql.NullString
		if err := rows.Scan(&e.Seq, &e.TenantID, &typ, &e.Resource, &e.Amount, &e.PolicyVersion, &e.CycleNumber, &resID, &idem, &ts); err != nil {
			return nil, err
		}
		e.Type = domain.EntryType(typ)
		e.ReservationID = resID.String
		e.IdempotencyKey = idem.String
		e.Ts = clock.Time(ts)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (t *sqliteTx) GetIdempotencyRecord(ctx context.Context, tenantID, key string) (IdempotencyRecord, error) {
	var r IdempotencyRecord
	var created int64
	var resp []byte
	err := t.tx.QueryRowContext(ctx,
		`SELECT tenant_id, key, request_hash, response, created_at FROM idempotency_records WHERE tenant_id=? AND key=?`,
		tenantID, key).Scan(&r.TenantID, &r.Key, &r.RequestHash, &resp, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	if err != nil {
		return r, err
	}
	r.Response = resp
	r.CreatedAt = clock.Time(created)
	return r, nil
}

func (t *sqliteTx) InsertIdempotencyRecord(ctx context.Context, r IdempotencyRecord) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO idempotency_records(tenant_id, key, request_hash, response, created_at) VALUES(?,?,?,?,?)
		 ON CONFLICT(tenant_id, key) DO NOTHING`,
		r.TenantID, r.Key, r.RequestHash, r.Response, int64(r.CreatedAt))
	return err
}

func (t *sqliteTx) GetCoordinatorState(ctx context.Context) (domain.CoordinatorState, error) {
	var s domain.CoordinatorState
	var lm int64
	var faulted int64
	err := t.tx.QueryRowContext(ctx,
		`SELECT value FROM coordinator_state WHERE key='last_maintenance'`).Scan(&lm)
	if errors.Is(err, sql.ErrNoRows) {
		// Fresh database: no state yet.
	} else if err != nil {
		return s, err
	} else {
		s.LastMaintenance = clock.Time(lm)
	}
	err = t.tx.QueryRowContext(ctx,
		`SELECT value FROM coordinator_state WHERE key='faulted'`).Scan(&faulted)
	if err == nil {
		s.Faulted = faulted != 0
	}
	err = t.tx.QueryRowContext(ctx,
		`SELECT value FROM coordinator_text WHERE key='fault_reason'`).Scan(&s.FaultReason)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return s, err
	}
	return s, nil
}

func (t *sqliteTx) SetCoordinatorState(ctx context.Context, s domain.CoordinatorState) error {
	if _, err := t.tx.ExecContext(ctx,
		`INSERT INTO coordinator_state(key, value) VALUES('last_maintenance', ?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`, int64(s.LastMaintenance)); err != nil {
		return err
	}
	faulted := int64(0)
	if s.Faulted {
		faulted = 1
	}
	if _, err := t.tx.ExecContext(ctx,
		`INSERT INTO coordinator_state(key, value) VALUES('faulted', ?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`, faulted); err != nil {
		return err
	}
	if _, err := t.tx.ExecContext(ctx,
		`INSERT INTO coordinator_text(key, value) VALUES('fault_reason', ?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`, s.FaultReason); err != nil {
		return err
	}
	return nil
}

func trimStmt(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\n' || s[start] == '\t' || s[start] == '\r' || s[start] == ';') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\n' || s[end-1] == '\t' || s[end-1] == '\r' || s[end-1] == ';') {
		end--
	}
	return s[start:end]
}
