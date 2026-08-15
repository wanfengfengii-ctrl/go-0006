// Package migrations holds the SQL schema for QuotaRaft's SQLite database.
//
// The schema is applied in order at startup. Each migration is a single
// statement block; the migrations table records which have run so that
// restarts are idempotent. The design follows an append-only ledger: the
// ledger_entries table is only ever inserted into, never updated or deleted,
// which is what makes the balance projection rebuildable.
package migrations

// Schema is the full DDL, executed inside one transaction at startup. Keeping
// the statements idempotent (CREATE TABLE IF NOT EXISTS, etc.) means a fresh
// database and an existing one are handled by the same code path.
const Schema = `
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS tenants (
  id TEXT PRIMARY KEY,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS policies (
  tenant_id     TEXT NOT NULL,
  resource      TEXT NOT NULL,
  version       INTEGER NOT NULL,
  type          TEXT NOT NULL,
  capacity      INTEGER NOT NULL DEFAULT 0,
  initial_tokens INTEGER NOT NULL DEFAULT 0,
  refill_amount INTEGER NOT NULL DEFAULT 0,
  refill_interval INTEGER NOT NULL DEFAULT 0,
  cycle_limit   INTEGER NOT NULL DEFAULT 0,
  cycle_length  INTEGER NOT NULL DEFAULT 0,
  cycle_anchor  INTEGER NOT NULL DEFAULT 0,
  created_at    INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, resource, version)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_policies_current ON policies(tenant_id, resource, version);

CREATE TABLE IF NOT EXISTS balances (
  tenant_id      TEXT NOT NULL,
  resource       TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  balance        INTEGER NOT NULL,
  frozen         INTEGER NOT NULL DEFAULT 0,
  last_refill_time INTEGER NOT NULL DEFAULT 0,
  current_cycle  INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, resource)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS reservations (
  id         TEXT PRIMARY KEY,
  tenant_id  TEXT NOT NULL,
  status     TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS reservation_items (
  reservation_id TEXT NOT NULL,
  resource       TEXT NOT NULL,
  amount         INTEGER NOT NULL,
  PRIMARY KEY (reservation_id, resource)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS ledger_entries (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant_id    TEXT NOT NULL,
  type         TEXT NOT NULL,
  resource     TEXT NOT NULL,
  amount       INTEGER NOT NULL,
  policy_version INTEGER NOT NULL,
  cycle_number INTEGER NOT NULL DEFAULT 0,
  reservation_id TEXT,
  idempotency_key TEXT,
  ts           INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ledger_tr ON ledger_entries(tenant_id, resource, seq);
CREATE INDEX IF NOT EXISTS idx_ledger_tenant_seq ON ledger_entries(tenant_id, seq);
CREATE INDEX IF NOT EXISTS idx_ledger_resv ON ledger_entries(reservation_id) WHERE reservation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS idempotency_records (
  tenant_id    TEXT NOT NULL,
  key          TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  response     BLOB NOT NULL,
  created_at   INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, key)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS coordinator_state (
  key   TEXT PRIMARY KEY,
  value INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS coordinator_text (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`

// Statements is the schema split into individually executable statements, used
// when a driver does not support multi-statement exec.
var Statements = splitStatements(Schema)

// CurrentVersion is the schema version recorded after applying the migrations.
const CurrentVersion = 1

func splitStatements(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ';' {
			stmt := s[start : i+1]
			out = append(out, stmt)
			start = i + 1
		}
	}
	// Trailing non-semicolon text is whitespace only.
	return out
}
