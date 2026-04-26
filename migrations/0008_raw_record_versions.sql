ALTER TABLE raw_records
  ADD COLUMN version INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_raw_records_version
  ON raw_records (version DESC);
