CREATE TABLE IF NOT EXISTS raw_records (
  id TEXT PRIMARY KEY,
  record_key TEXT NOT NULL UNIQUE,
  source TEXT NOT NULL DEFAULT 'downstream',
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  received_at TEXT NOT NULL,
  processed_at TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS secure_records (
  id TEXT PRIMARY KEY,
  raw_record_id TEXT NOT NULL,
  record_key TEXT NOT NULL UNIQUE,
  version INTEGER NOT NULL,
  key_version INTEGER NOT NULL DEFAULT 1,
  public_json TEXT NOT NULL,
  encrypted_json TEXT NOT NULL,
  encrypt_fields_json TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  synced_at TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY (raw_record_id) REFERENCES raw_records(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS downstream_clients (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_name TEXT,
  callback_url TEXT NOT NULL UNIQUE,
  client_version INTEGER NOT NULL DEFAULT 0,
  last_sync_version INTEGER NOT NULL DEFAULT 0,
  last_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  last_push_at TEXT,
  last_status TEXT NOT NULL DEFAULT 'pending',
  last_response_code INTEGER,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_records_record_key
  ON raw_records (record_key);

CREATE INDEX IF NOT EXISTS idx_secure_records_version
  ON secure_records (version DESC);

CREATE INDEX IF NOT EXISTS idx_secure_records_record_key
  ON secure_records (record_key);

CREATE INDEX IF NOT EXISTS idx_downstream_clients_seen_at
  ON downstream_clients (last_seen_at DESC);
