ALTER TABLE downstream_clients
  ADD COLUMN last_pull_version INTEGER NOT NULL DEFAULT 0;

ALTER TABLE downstream_clients
  ADD COLUMN last_pull_at TEXT;

ALTER TABLE downstream_clients
  ADD COLUMN last_pull_status TEXT;

ALTER TABLE downstream_clients
  ADD COLUMN last_pull_response_code INTEGER;

ALTER TABLE downstream_clients
  ADD COLUMN last_pull_error TEXT;

CREATE INDEX IF NOT EXISTS idx_downstream_clients_pull_priority
  ON downstream_clients (entry_kind, databack_version DESC, last_seen_at DESC);
