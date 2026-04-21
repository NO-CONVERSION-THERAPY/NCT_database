export type JsonPrimitive = string | number | boolean | null;

export type JsonValue =
  | JsonPrimitive
  | JsonObject
  | JsonValue[];

export type JsonObject = {
  [key: string]: JsonValue;
};

export interface EncryptedEnvelope {
  algorithm: 'AES-GCM';
  iv: string;
  ciphertext: string;
}

export interface SecureTransferPayload {
  keyVersion: number;
  publicData: JsonObject;
  encryptedData: EncryptedEnvelope;
  encryptFields: string[];
  syncedAt: string | null;
}

export interface RawRecord {
  id: string;
  recordKey: string;
  source: string;
  payload: JsonObject;
  payloadColumns: Record<string, string | null>;
  payloadHash: string;
  receivedAt: string;
  processedAt: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface SecureRecord {
  id: string;
  rawRecordId: string;
  recordKey: string;
  version: number;
  keyVersion: number;
  publicData: JsonObject;
  publicColumns: Record<string, string | null>;
  encryptedData: EncryptedEnvelope;
  encryptedColumns: Record<string, string | null>;
  encryptFields: string[];
  fingerprint: string;
  syncedAt: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface DownstreamClient {
  id: number;
  entryKind: string;
  clientName: string | null;
  callbackUrl: string;
  clientVersion: number;
  lastSyncVersion: number;
  lastSeenAt: string;
  lastPushAt: string | null;
  lastStatus: string;
  lastResponseCode: number | null;
  lastError: string | null;
  serviceUrl: string | null;
  databackVersion: number | null;
  reportCount: number | null;
  reportedAt: string | null;
  payload: JsonObject | null;
  lastPullVersion: number;
  lastPullAt: string | null;
  lastPullStatus: string | null;
  lastPullResponseCode: number | null;
  lastPullError: string | null;
}

export interface SubReportPayload {
  service: string;
  serviceUrl: string;
  databackVersion: number | null;
  reportCount: number;
  reportedAt: string;
}

export interface IngestRecordInput {
  recordKey?: string;
  source?: string;
  encryptFields?: string[];
  payload: JsonObject;
}

export interface IngestRequest {
  records: IngestRecordInput[];
}

export interface IngestResult {
  recordKey: string;
  rawRecordId: string;
  secureRecordId: string;
  version: number;
  updated: boolean;
}

export interface SyncRequest {
  clientName?: string;
  callbackUrl: string;
  currentVersion: number;
  mode?: 'full' | 'delta';
}

export interface SyncPayload {
  mode: 'full' | 'delta';
  previousVersion: number;
  currentVersion: number;
  totalRecords: number;
  records: SecureRecord[];
  generatedAt: string;
}

export interface SubPushRecord {
  recordKey: string;
  version: number;
  fingerprint: string;
  payload: SecureTransferPayload;
}

export interface SubPushPayload {
  service: string;
  mode: 'full' | 'delta';
  previousVersion: number;
  currentVersion: number;
  totalRecords: number;
  records: SubPushRecord[];
  generatedAt: string;
}

export interface SubDatabackExportRecord {
  recordKey: string;
  version: number;
  fingerprint: string;
  payload: SecureTransferPayload;
  updatedAt: string;
}

export interface SubDatabackExportFile {
  service: string;
  serviceUrl: string;
  afterVersion: number;
  currentVersion: number | null;
  exportedAt: string;
  totalRecords: number;
  records: SubDatabackExportRecord[];
}

export interface AnalyticsOverview {
  totals: {
    rawRecords: number;
    secureRecords: number;
    downstreamClients: number;
    currentVersion: number;
  };
  rawBySource: Array<{
    source: string;
    count: number;
  }>;
  syncStatuses: Array<{
    status: string;
    count: number;
  }>;
  versionHistory: Array<{
    recordKey: string;
    version: number;
  }>;
}

export interface AdminSnapshot {
  overview: AnalyticsOverview;
  rawRecords: RawRecord[];
  secureRecords: SecureRecord[];
  downstreamClients: DownstreamClient[];
}

export interface PublicDatasetStatistic {
  province: string;
  count: number;
}

export interface PublicDatasetItem {
  name: string;
  addr: string;
  province: string;
  prov: string;
  else: string;
  lat: number | null;
  lng: number | null;
  experience: string;
  HMaster: string;
  scandal: string;
  contact: string;
  inputType: string;
}

export interface PublicDatasetResponse {
  avg_age: number;
  last_synced: number;
  statistics: PublicDatasetStatistic[];
  data: PublicDatasetItem[];
}
