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
  RESEND_API_KEY?: string;
  EXPORT_EMAIL_TO?: string;
  EXPORT_EMAIL_FROM?: string;
  EXPORT_WEBHOOK_URL?: string;
  EXPORT_TIMEZONE?: string;
}
