ALTER TABLE downstream_clients
  ADD COLUMN entry_kind TEXT NOT NULL DEFAULT 'sync-client';

ALTER TABLE downstream_clients
  ADD COLUMN service_url TEXT;

ALTER TABLE downstream_clients
  ADD COLUMN databack_version INTEGER;

ALTER TABLE downstream_clients
  ADD COLUMN report_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE downstream_clients
  ADD COLUMN reported_at TEXT;

ALTER TABLE downstream_clients
  ADD COLUMN payload_json TEXT;

CREATE INDEX IF NOT EXISTS idx_downstream_clients_entry_kind_seen_at
  ON downstream_clients (entry_kind, last_seen_at DESC);
