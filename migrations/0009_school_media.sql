CREATE TABLE IF NOT EXISTS media_tags (
  id TEXT PRIMARY KEY,
  slug TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL,
  normalized_label TEXT NOT NULL UNIQUE,
  is_system INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS school_media (
  id TEXT PRIMARY KEY,
  source_service_url TEXT NOT NULL,
  source_media_id TEXT NOT NULL,
  object_key TEXT NOT NULL,
  public_url TEXT NOT NULL,
  media_type TEXT NOT NULL CHECK (media_type IN ('image', 'video')),
  content_type TEXT NOT NULL,
  byte_size INTEGER NOT NULL,
  file_name TEXT NOT NULL,
  school_name TEXT NOT NULL,
  school_name_norm TEXT NOT NULL,
  school_address TEXT NOT NULL DEFAULT '',
  province TEXT NOT NULL DEFAULT '',
  city TEXT NOT NULL DEFAULT '',
  county TEXT NOT NULL DEFAULT '',
  is_r18 INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'pending_review' CHECK (status IN ('pending_review', 'approved', 'rejected')),
  review_note TEXT,
  uploaded_at TEXT,
  reviewed_at TEXT,
  source_updated_at TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  UNIQUE (source_service_url, source_media_id)
);

CREATE TABLE IF NOT EXISTS school_media_tags (
  media_id TEXT NOT NULL,
  tag_id TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  PRIMARY KEY (media_id, tag_id),
  FOREIGN KEY (media_id) REFERENCES school_media(id) ON DELETE CASCADE,
  FOREIGN KEY (tag_id) REFERENCES media_tags(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_school_media_status
  ON school_media (status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_school_media_school
  ON school_media (school_name_norm, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_school_media_r18
  ON school_media (is_r18, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_school_media_source
  ON school_media (source_service_url, source_media_id);

CREATE INDEX IF NOT EXISTS idx_media_tags_slug
  ON media_tags (slug);

INSERT INTO media_tags (
  id,
  slug,
  label,
  normalized_label,
  is_system
)
VALUES (
  'tag:r18',
  'r18',
  'R18',
  'r18',
  1
)
ON CONFLICT(slug) DO UPDATE SET
  label = excluded.label,
  normalized_label = excluded.normalized_label,
  is_system = 1,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now');
