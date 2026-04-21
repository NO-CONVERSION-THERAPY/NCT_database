interface Env {
  ASSETS: Fetcher;
  DB: D1Database;
  EXPORT_BUCKET: R2Bucket;
  APP_NAME?: string;
  DEFAULT_ENCRYPT_FIELDS?: string;
  ENCRYPTION_KEY: string;
  ENCRYPTION_KEY_VERSION?: string;
  ADMIN_TOKEN?: string;
  INGEST_TOKEN?: string;
  SYNC_TOKEN?: string;
  SUB_REPORT_TOKEN?: string;
  SUB_PUSH_TOKEN?: string;
  SUB_REPORT_MIN_INTERVAL_MS?: string;
  SUB_PULL_BATCH_SIZE?: string;
  SUB_PULL_RECORD_LIMIT?: string;
  SUB_PULL_TIMEOUT_MS?: string;
  RESEND_API_KEY?: string;
  EXPORT_EMAIL_TO?: string;
  EXPORT_EMAIL_FROM?: string;
  EXPORT_WEBHOOK_URL?: string;
  EXPORT_TIMEZONE?: string;
}
