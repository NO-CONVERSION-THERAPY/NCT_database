import type {
  AdminSnapshot,
  AnalyticsOverview,
  DownstreamClient,
  IngestRecordInput,
  IngestResult,
  JsonObject,
  JsonValue,
  PublicDatasetItem,
  PublicDatasetResponse,
  RawRecord,
  SecureRecord,
  SecureTransferPayload,
  SubDatabackExportFile,
  SubPushPayload,
  SubPushRecord,
  SubReportPayload,
  SyncPayload,
  SyncRequest,
} from '../../shared/types';
import { decryptObject, encryptObject, sha256 } from './crypto';
import {
  ensureDynamicColumns,
  extractDynamicColumns,
  serializeDynamicColumnValue,
} from './dynamic-schema';
import {
  parseJsonObject,
  parseStringArray,
  stableStringify,
  toJsonObject,
} from './json';

type RawRecordRow = Record<string, unknown> & {
  id: string;
  record_key: string;
  source: string;
  payload_json: string;
  payload_hash: string;
  received_at: string;
  processed_at: string | null;
  created_at: string;
  updated_at: string;
};

type SecureRecordRow = Record<string, unknown> & {
  id: string;
  raw_record_id: string;
  record_key: string;
  version: number;
  key_version: number;
  public_json: string;
  encrypted_json: string;
  encrypt_fields_json: string;
  fingerprint: string;
  synced_at: string | null;
  created_at: string;
  updated_at: string;
};

type DownstreamClientRow = {
  id: number;
  entry_kind: string | null;
  client_name: string | null;
  callback_url: string;
  client_version: number;
  last_sync_version: number;
  last_seen_at: string;
  last_push_at: string | null;
  last_status: string;
  last_response_code: number | null;
  last_error: string | null;
  service_url: string | null;
  databack_version: number | null;
  report_count: number | null;
  reported_at: string | null;
  payload_json: string | null;
  last_pull_version: number | null;
  last_pull_at: string | null;
  last_pull_status: string | null;
  last_pull_response_code: number | null;
  last_pull_error: string | null;
};

type ColumnAssignment = {
  column: string;
  value: string | null;
};

const RECOGNIZED_SUB_SERVICES = new Set([
  'NCT API SQL Sub',
  'nct-api-sql-sub',
]);
const SUB_PUSH_PATH = '/api/push/secure-records';
const SUB_EXPORT_PATH = '/api/export/nct_databack';

function nowIso(): string {
  return new Date().toISOString();
}

function readStringField(
  payload: JsonObject,
  fieldName: string,
): string {
  const value = payload[fieldName];

  if (typeof value === 'string') {
    return value;
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }

  return '';
}

function readNumberField(
  payload: JsonObject,
  fieldName: string,
): number | null {
  const value = payload[fieldName];

  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string') {
    const parsed = Number(value.trim());
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return null;
}

function mapPublicDatasetItem(
  record: RawRecord,
): PublicDatasetItem {
  const payload = record.payload;

  return {
    name: readStringField(payload, 'name'),
    addr: readStringField(payload, 'addr'),
    province: readStringField(payload, 'province'),
    prov: readStringField(payload, 'prov'),
    else: readStringField(payload, 'else'),
    lat: readNumberField(payload, 'lat'),
    lng: readNumberField(payload, 'lng'),
    experience: readStringField(payload, 'experience'),
    HMaster: readStringField(payload, 'HMaster'),
    scandal: readStringField(payload, 'scandal'),
    contact: readStringField(payload, 'contact'),
    inputType: readStringField(payload, 'inputType'),
  };
}

function quoteIdentifier(
  identifier: string,
): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function buildInsertStatement(
  tableName: string,
  columns: string[],
): string {
  return `
    INSERT INTO ${quoteIdentifier(tableName)} (
      ${columns.map((column) => quoteIdentifier(column)).join(', ')}
    )
    VALUES (${columns.map(() => '?').join(', ')})
  `;
}

function buildUpdateStatement(
  tableName: string,
  columns: string[],
  whereColumn: string,
): string {
  return `
    UPDATE ${quoteIdentifier(tableName)}
    SET ${columns
      .map((column) => `${quoteIdentifier(column)} = ?`)
      .join(', ')}
    WHERE ${quoteIdentifier(whereColumn)} = ?
  `;
}

function getDefaultEncryptFields(env: Env): string[] {
  return (env.DEFAULT_ENCRYPT_FIELDS ?? '')
    .split(',')
    .map((field) => field.trim())
    .filter(Boolean);
}

function readRecordKey(
  input: IngestRecordInput,
): string {
  const payload = input.payload as Record<string, unknown>;

  const candidates = [
    input.recordKey,
    typeof payload.recordKey === 'string' ? payload.recordKey : undefined,
    typeof payload.id === 'string' ? payload.id : undefined,
    typeof payload.externalId === 'string' ? payload.externalId : undefined,
    typeof payload.code === 'string' ? payload.code : undefined,
  ];

  const resolved = candidates.find(
    (candidate) => typeof candidate === 'string' && candidate.trim().length > 0,
  );

  return resolved ?? crypto.randomUUID();
}

function partitionPayload(
  payload: JsonObject,
  encryptFields: string[],
): {
  publicData: JsonObject;
  secretData: JsonObject;
} {
  const encryptedFieldSet = new Set(encryptFields);
  const publicData: JsonObject = {};
  const secretData: JsonObject = {};

  Object.entries(payload).forEach(([key, value]) => {
    if (encryptedFieldSet.has(key)) {
      secretData[key] = value;
      return;
    }

    publicData[key] = value;
  });

  return {
    publicData,
    secretData,
  };
}

async function getCurrentVersion(
  db: D1Database,
): Promise<number> {
  const result = await db
    .prepare(
      'SELECT COALESCE(MAX(version), 0) AS version FROM secure_records',
    )
    .first<{ version: number | null }>();

  return Number(result?.version ?? 0);
}

function collectFieldNames(
  ...groups: Iterable<string>[]
): string[] {
  const fieldNames = new Set<string>();

  groups.forEach((group) => {
    for (const fieldName of group) {
      const trimmedFieldName = fieldName.trim();
      if (trimmedFieldName) {
        fieldNames.add(trimmedFieldName);
      }
    }
  });

  return Array.from(fieldNames).sort((left, right) =>
    left.localeCompare(right),
  );
}

function hasOwn(
  value: object,
  key: string,
): boolean {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function buildDynamicAssignments(
  mappings: Map<string, string>,
  fieldNames: Iterable<string>,
  values: Record<string, JsonValue>,
): ColumnAssignment[] {
  return collectFieldNames(fieldNames).flatMap((fieldName) => {
    const column = mappings.get(fieldName);
    if (!column) {
      return [];
    }

    return [
      {
        column,
        value: hasOwn(values, fieldName)
          ? serializeDynamicColumnValue(values[fieldName])
          : null,
      },
    ];
  });
}

function mapRawRecord(
  row: RawRecordRow,
): RawRecord {
  const payload = parseJsonObject(row.payload_json);

  return {
    id: row.id,
    recordKey: row.record_key,
    source: row.source,
    payload,
    payloadColumns: extractDynamicColumns(
      row,
      'payload',
      Object.keys(payload),
    ),
    payloadHash: row.payload_hash,
    receivedAt: row.received_at,
    processedAt: row.processed_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapSecureRecord(
  row: SecureRecordRow,
): SecureRecord {
  const publicData = parseJsonObject(row.public_json);
  const encryptFields = parseStringArray(row.encrypt_fields_json);

  return {
    id: row.id,
    rawRecordId: row.raw_record_id,
    recordKey: row.record_key,
    version: row.version,
    keyVersion: row.key_version,
    publicData,
    publicColumns: extractDynamicColumns(
      row,
      'public',
      Object.keys(publicData),
    ),
    encryptedData: JSON.parse(row.encrypted_json),
    encryptedColumns: extractDynamicColumns(
      row,
      'encrypted',
      encryptFields,
    ),
    encryptFields,
    fingerprint: row.fingerprint,
    syncedAt: row.synced_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapDownstreamClient(
  row: DownstreamClientRow,
): DownstreamClient {
  const entryKind = row.entry_kind?.trim() || 'sync-client';

  return {
    id: row.id,
    entryKind,
    clientName: row.client_name,
    callbackUrl:
      entryKind === 'sub-report'
        ? row.service_url ?? row.callback_url
        : row.callback_url,
    clientVersion: row.client_version,
    lastSyncVersion: row.last_sync_version,
    lastSeenAt: row.last_seen_at,
    lastPushAt: row.last_push_at,
    lastStatus: row.last_status,
    lastResponseCode: row.last_response_code,
    lastError: row.last_error,
    serviceUrl: row.service_url,
    databackVersion:
      row.databack_version === null || row.databack_version === undefined
        ? null
        : Number(row.databack_version),
    reportCount:
      row.report_count === null || row.report_count === undefined
        ? null
        : Number(row.report_count),
    reportedAt: row.reported_at,
    payload: row.payload_json ? parseJsonObject(row.payload_json) : null,
    lastPullVersion: Number(row.last_pull_version ?? 0),
    lastPullAt: row.last_pull_at,
    lastPullStatus: row.last_pull_status,
    lastPullResponseCode: row.last_pull_response_code,
    lastPullError: row.last_pull_error,
  };
}

function buildSubReportKey(serviceUrl: string): string {
  return `sub-report:${serviceUrl}`;
}

function buildSubPushUrl(serviceUrl: string): string {
  return new URL(SUB_PUSH_PATH, serviceUrl).toString();
}

function buildSubExportUrl(serviceUrl: string): string {
  return new URL(SUB_EXPORT_PATH, serviceUrl).toString();
}

function readReportedSubVersion(
  row: DownstreamClientRow,
): number {
  return Math.max(
    0,
    Number(row.databack_version ?? 0),
    Number(row.client_version ?? 0),
    Number(row.last_sync_version ?? 0),
  );
}

function readSubPullVersion(
  row: DownstreamClientRow,
): number {
  return Math.max(0, Number(row.last_pull_version ?? 0));
}

function readPushedMainVersion(
  row: DownstreamClientRow,
): number {
  return Math.max(0, Number(row.last_sync_version ?? 0));
}

function readIsoMs(value: string | null | undefined): number | null {
  if (!value) {
    return null;
  }

  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function buildSecureTransferPayload(
  record: SecureRecord,
): SecureTransferPayload {
  return {
    keyVersion: record.keyVersion,
    publicData: record.publicData,
    encryptedData: record.encryptedData,
    encryptFields: record.encryptFields,
    syncedAt: record.syncedAt,
  };
}

function mapSecureRecordToSubPushRecord(
  record: SecureRecord,
): SubPushRecord {
  return {
    recordKey: record.recordKey,
    version: record.version,
    fingerprint: record.fingerprint,
    payload: buildSecureTransferPayload(record),
  };
}

export function isRecognizedSubService(
  serviceName: string,
): boolean {
  return RECOGNIZED_SUB_SERVICES.has(serviceName.trim());
}

export async function listRawRecords(
  db: D1Database,
  limit?: number,
): Promise<RawRecord[]> {
  const statement =
    limit === undefined
      ? db.prepare(
          `
            SELECT *
            FROM raw_records
            ORDER BY updated_at DESC
          `,
        )
      : db
          .prepare(
            `
              SELECT *
              FROM raw_records
              ORDER BY updated_at DESC
              LIMIT ?
            `,
          )
          .bind(limit);
  const result = await statement.all<RawRecordRow>();

  return (result.results ?? []).map(mapRawRecord);
}

export async function listSecureRecords(
  db: D1Database,
  limit?: number,
): Promise<SecureRecord[]> {
  const statement =
    limit === undefined
      ? db.prepare(
          `
            SELECT *
            FROM secure_records
            ORDER BY version DESC, updated_at DESC
          `,
        )
      : db
          .prepare(
            `
              SELECT *
              FROM secure_records
              ORDER BY version DESC, updated_at DESC
              LIMIT ?
            `,
          )
          .bind(limit);
  const result = await statement.all<SecureRecordRow>();

  return (result.results ?? []).map(mapSecureRecord);
}

export async function listDownstreamClients(
  db: D1Database,
  limit?: number,
): Promise<DownstreamClient[]> {
  const statement =
    limit === undefined
      ? db.prepare(
          `
            SELECT *
            FROM downstream_clients
            ORDER BY last_seen_at DESC
          `,
        )
      : db
          .prepare(
            `
              SELECT *
              FROM downstream_clients
              ORDER BY last_seen_at DESC
              LIMIT ?
            `,
          )
          .bind(limit);
  const result = await statement.all<DownstreamClientRow>();

  return (result.results ?? []).map(mapDownstreamClient);
}

export async function ingestRecords(
  env: Env,
  records: IngestRecordInput[],
): Promise<IngestResult[]> {
  if (!records.length) {
    return [];
  }

  const keyVersion = Number(env.ENCRYPTION_KEY_VERSION ?? '1');
  const results: IngestResult[] = [];

  for (const rawInput of records) {
    const payload = toJsonObject(rawInput.payload);
    const recordKey = readRecordKey(rawInput);
    const source = rawInput.source?.trim() || 'downstream';
    const payloadJson = stableStringify(payload);
    const payloadHash = await sha256(payloadJson);
    const receivedAt = nowIso();
    const existingRaw = await env.DB.prepare(
      `
        SELECT *
        FROM raw_records
        WHERE record_key = ?
      `,
    )
      .bind(recordKey)
      .first<RawRecordRow>();
    const existingSecure = await env.DB.prepare(
      `
        SELECT *
        FROM secure_records
        WHERE record_key = ?
      `,
    )
      .bind(recordKey)
      .first<SecureRecordRow>();
    const previousPayload = existingRaw
      ? parseJsonObject(existingRaw.payload_json)
      : {};
    const previousFieldNames = Object.keys(previousPayload);
    const previousEncryptFields = existingSecure
      ? parseStringArray(existingSecure.encrypt_fields_json)
      : [];
    const previousEncryptedFieldSet = new Set(previousEncryptFields);
    const previousPublicFieldNames = previousFieldNames.filter(
      (fieldName) => !previousEncryptedFieldSet.has(fieldName),
    );
    const previousEncryptedFieldNames = previousFieldNames.filter(
      (fieldName) => previousEncryptedFieldSet.has(fieldName),
    );
    const encryptFields = Array.from(
      new Set(
        (rawInput.encryptFields?.length
          ? rawInput.encryptFields
          : getDefaultEncryptFields(env)
        ).map((field) => field.trim()).filter(Boolean),
      ),
    );
    const { publicData, secretData } = partitionPayload(
      payload,
      encryptFields,
    );
    const rawFieldNames = collectFieldNames(
      previousFieldNames,
      Object.keys(payload),
    );
    const publicFieldNames = collectFieldNames(
      previousPublicFieldNames,
      Object.keys(publicData),
    );
    const encryptedFieldNames = collectFieldNames(
      previousEncryptedFieldNames,
      Object.keys(secretData),
    );
    const rawColumnMappings = await ensureDynamicColumns(
      env.DB,
      'raw_records',
      'payload',
      rawFieldNames,
    );
    const publicColumnMappings = await ensureDynamicColumns(
      env.DB,
      'secure_records',
      'public',
      publicFieldNames,
    );
    const encryptedColumnMappings = await ensureDynamicColumns(
      env.DB,
      'secure_records',
      'encrypted',
      encryptedFieldNames,
    );
    const publicJson = stableStringify(publicData);
    const encryptedEnvelope = await encryptObject(
      secretData,
      env.ENCRYPTION_KEY,
    );
    const encryptedJson = stableStringify(encryptedEnvelope);
    const encryptedColumnValues: JsonObject = {};

    for (const [fieldName, fieldValue] of Object.entries(secretData)) {
      encryptedColumnValues[fieldName] = stableStringify(
        await encryptObject(
          { [fieldName]: fieldValue },
          env.ENCRYPTION_KEY,
        ),
      );
    }

    const fingerprint = await sha256(
      stableStringify({
        keyVersion,
        publicData,
        secretData,
        encryptFields,
      }),
    );

    const rawRecordId = existingRaw?.id ?? crypto.randomUUID();
    const rawDynamicAssignments = buildDynamicAssignments(
      rawColumnMappings,
      rawFieldNames,
      payload,
    );

    if (existingRaw) {
      const updateColumns = [
        'source',
        'payload_json',
        'payload_hash',
        'received_at',
        'updated_at',
        ...rawDynamicAssignments.map(
          (assignment) => assignment.column,
        ),
      ];
      const updateValues = [
        source,
        payloadJson,
        payloadHash,
        receivedAt,
        receivedAt,
        ...rawDynamicAssignments.map(
          (assignment) => assignment.value,
        ),
        rawRecordId,
      ];

      await env.DB.prepare(
        buildUpdateStatement(
          'raw_records',
          updateColumns,
          'id',
        ),
      )
        .bind(...updateValues)
        .run();
    } else {
      const insertColumns = [
        'id',
        'record_key',
        'source',
        'payload_json',
        'payload_hash',
        'received_at',
        'created_at',
        'updated_at',
        ...rawDynamicAssignments.map(
          (assignment) => assignment.column,
        ),
      ];
      const insertValues = [
        rawRecordId,
        recordKey,
        source,
        payloadJson,
        payloadHash,
        receivedAt,
        receivedAt,
        receivedAt,
        ...rawDynamicAssignments.map(
          (assignment) => assignment.value,
        ),
      ];

      await env.DB.prepare(
        buildInsertStatement(
          'raw_records',
          insertColumns,
        ),
      )
        .bind(...insertValues)
        .run();
    }

    const secureRecordId = existingSecure?.id ?? crypto.randomUUID();
    const currentVersion = await getCurrentVersion(env.DB);
    const hasChanged = existingSecure?.fingerprint !== fingerprint;
    const nextVersion = existingSecure
      ? hasChanged
        ? currentVersion + 1
        : existingSecure.version
      : currentVersion + 1;
    const publicDynamicAssignments = buildDynamicAssignments(
      publicColumnMappings,
      publicFieldNames,
      publicData,
    );
    const encryptedDynamicAssignments = buildDynamicAssignments(
      encryptedColumnMappings,
      encryptedFieldNames,
      encryptedColumnValues,
    );

    if (!existingSecure) {
      const insertColumns = [
        'id',
        'raw_record_id',
        'record_key',
        'version',
        'key_version',
        'public_json',
        'encrypted_json',
        'encrypt_fields_json',
        'fingerprint',
        'created_at',
        'updated_at',
        ...publicDynamicAssignments.map(
          (assignment) => assignment.column,
        ),
        ...encryptedDynamicAssignments.map(
          (assignment) => assignment.column,
        ),
      ];
      const insertValues = [
        secureRecordId,
        rawRecordId,
        recordKey,
        nextVersion,
        keyVersion,
        publicJson,
        encryptedJson,
        stableStringify(encryptFields),
        fingerprint,
        receivedAt,
        receivedAt,
        ...publicDynamicAssignments.map(
          (assignment) => assignment.value,
        ),
        ...encryptedDynamicAssignments.map(
          (assignment) => assignment.value,
        ),
      ];

      await env.DB.prepare(
        buildInsertStatement(
          'secure_records',
          insertColumns,
        ),
      )
        .bind(...insertValues)
        .run();
    } else if (hasChanged) {
      const updateColumns = [
        'raw_record_id',
        'version',
        'key_version',
        'public_json',
        'encrypted_json',
        'encrypt_fields_json',
        'fingerprint',
        'updated_at',
        ...publicDynamicAssignments.map(
          (assignment) => assignment.column,
        ),
        ...encryptedDynamicAssignments.map(
          (assignment) => assignment.column,
        ),
      ];
      const updateValues = [
        rawRecordId,
        nextVersion,
        keyVersion,
        publicJson,
        encryptedJson,
        stableStringify(encryptFields),
        fingerprint,
        receivedAt,
        ...publicDynamicAssignments.map(
          (assignment) => assignment.value,
        ),
        ...encryptedDynamicAssignments.map(
          (assignment) => assignment.value,
        ),
        secureRecordId,
      ];

      await env.DB.prepare(
        buildUpdateStatement(
          'secure_records',
          updateColumns,
          'id',
        ),
      )
        .bind(...updateValues)
        .run();
    }

    await env.DB.prepare(
      `
        UPDATE raw_records
        SET processed_at = ?,
            updated_at = ?
        WHERE id = ?
      `,
    )
      .bind(receivedAt, receivedAt, rawRecordId)
      .run();

    results.push({
      recordKey,
      rawRecordId,
      secureRecordId,
      version: nextVersion,
      updated: !existingSecure || hasChanged,
    });
  }

  return results;
}

export async function rebuildSecureRecords(
  env: Env,
): Promise<IngestResult[]> {
  const rawRecords = await listRawRecords(env.DB);

  return ingestRecords(
    env,
    rawRecords.map((record) => ({
      recordKey: record.recordKey,
      source: record.source,
      payload: record.payload,
    })),
  );
}

export async function getPublishedPayload(
  db: D1Database,
  currentVersion: number,
  requestedVersion: number,
  mode: 'full' | 'delta',
): Promise<SyncPayload> {
  const query =
    mode === 'delta'
      ? `
          SELECT *
          FROM secure_records
          WHERE version > ?
          ORDER BY version ASC
        `
      : `
          SELECT *
          FROM secure_records
          ORDER BY version ASC
        `;

  const statement = db.prepare(query);
  const result =
    mode === 'delta'
      ? await statement.bind(requestedVersion).all<SecureRecordRow>()
      : await statement.all<SecureRecordRow>();

  const records = (result.results ?? []).map(mapSecureRecord);

  return {
    mode,
    previousVersion: requestedVersion,
    currentVersion,
    totalRecords: records.length,
    records,
    generatedAt: nowIso(),
  };
}

async function buildSubPushPayload(
  db: D1Database,
  currentVersion: number,
  previousVersion: number,
): Promise<SubPushPayload> {
  const published = await getPublishedPayload(
    db,
    currentVersion,
    previousVersion,
    'delta',
  );

  return {
    service: 'NCT API SQL',
    mode: published.mode,
    previousVersion: published.previousVersion,
    currentVersion: published.currentVersion,
    totalRecords: published.totalRecords,
    records: published.records.map(mapSecureRecordToSubPushRecord),
    generatedAt: published.generatedAt,
  };
}

function createJsonAttachmentFormData(
  filename: string,
  payload: unknown,
): FormData {
  const formData = new FormData();
  formData.append(
    'file',
    new Blob([stableStringify(toJsonObject(payload as Record<string, unknown>))], {
      type: 'application/json',
    }),
    filename,
  );

  return formData;
}

function isSecureTransferPayload(
  value: unknown,
): value is SecureTransferPayload {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.keyVersion === 'number'
    && Number.isFinite(candidate.keyVersion)
    && !!candidate.publicData
    && typeof candidate.publicData === 'object'
    && !Array.isArray(candidate.publicData)
    && !!candidate.encryptedData
    && typeof candidate.encryptedData === 'object'
    && !Array.isArray(candidate.encryptedData)
    && Array.isArray(candidate.encryptFields)
    && candidate.encryptFields.every((field) => typeof field === 'string')
    && (typeof candidate.syncedAt === 'string' || candidate.syncedAt === null)
  );
}

function parseSubDatabackExportFile(
  value: unknown,
): SubDatabackExportFile {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Sub export file must be a JSON object.');
  }

  const candidate = value as Record<string, unknown>;
  if (
    typeof candidate.service !== 'string'
    || typeof candidate.serviceUrl !== 'string'
    || typeof candidate.afterVersion !== 'number'
    || (candidate.currentVersion !== null
      && typeof candidate.currentVersion !== 'number')
    || typeof candidate.exportedAt !== 'string'
    || typeof candidate.totalRecords !== 'number'
    || !Array.isArray(candidate.records)
  ) {
    throw new Error('Sub export file is missing required fields.');
  }

  const records = candidate.records.map((record) => {
    if (!record || typeof record !== 'object' || Array.isArray(record)) {
      throw new Error('Sub export record must be a JSON object.');
    }

    const entry = record as Record<string, unknown>;
    if (
      typeof entry.recordKey !== 'string'
      || typeof entry.version !== 'number'
      || typeof entry.fingerprint !== 'string'
      || typeof entry.updatedAt !== 'string'
      || !isSecureTransferPayload(entry.payload)
    ) {
      throw new Error('Sub export record has an invalid shape.');
    }

    return {
      recordKey: entry.recordKey,
      version: entry.version,
      fingerprint: entry.fingerprint,
      payload: entry.payload,
      updatedAt: entry.updatedAt,
    };
  });

  return {
    service: candidate.service,
    serviceUrl: candidate.serviceUrl,
    afterVersion: candidate.afterVersion,
    currentVersion: candidate.currentVersion,
    exportedAt: candidate.exportedAt,
    totalRecords: candidate.totalRecords,
    records,
  };
}

async function importSubDatabackExportFile(
  env: Env,
  serviceUrl: string,
  exportFile: SubDatabackExportFile,
): Promise<{
  receivedCount: number;
  importedCount: number;
  updatedCount: number;
  skippedCount: number;
  highestPulledVersion: number;
}> {
  let updatedCount = 0;
  let skippedCount = 0;
  let highestPulledVersion = Math.max(0, exportFile.afterVersion);

  for (const record of exportFile.records) {
    highestPulledVersion = Math.max(
      highestPulledVersion,
      Math.max(0, Number(record.version)),
    );
    const existingSecure = await env.DB.prepare(
      `
        SELECT fingerprint
        FROM secure_records
        WHERE record_key = ?
      `,
    )
      .bind(record.recordKey)
      .first<{ fingerprint: string | null }>();

    if (existingSecure?.fingerprint === record.fingerprint) {
      skippedCount += 1;
      continue;
    }

    const secretData = await decryptObject(
      record.payload.encryptedData,
      env.ENCRYPTION_KEY,
    );
    const fingerprint = await sha256(
      stableStringify({
        keyVersion: record.payload.keyVersion,
        publicData: record.payload.publicData,
        secretData,
        encryptFields: record.payload.encryptFields,
      }),
    );

    if (fingerprint !== record.fingerprint) {
      throw new Error(
        `Fingerprint mismatch while importing ${record.recordKey} from ${serviceUrl}.`,
      );
    }

    const payload: JsonObject = {
      ...record.payload.publicData,
      ...secretData,
    };
    const [result] = await ingestRecords(env, [
      {
        recordKey: record.recordKey,
        source: `sub:${serviceUrl}`,
        encryptFields: record.payload.encryptFields,
        payload,
      },
    ]);

    if (result?.updated) {
      updatedCount += 1;
    } else {
      skippedCount += 1;
    }
  }

  return {
    receivedCount: exportFile.records.length,
    importedCount: exportFile.records.length,
    updatedCount,
    skippedCount,
    highestPulledVersion,
  };
}

export async function getSubReportThrottleState(
  db: D1Database,
  serviceUrl: string,
  minIntervalMs: number,
): Promise<{
  retryAfterMs: number;
  lastSeenAt: string | null;
} | null> {
  if (minIntervalMs <= 0) {
    return null;
  }

  const existing = await db.prepare(
    `
      SELECT last_seen_at
      FROM downstream_clients
      WHERE callback_url = ?
    `,
  )
    .bind(buildSubReportKey(serviceUrl))
    .first<{ last_seen_at: string | null }>();

  const lastSeenMs = readIsoMs(existing?.last_seen_at);
  if (lastSeenMs === null) {
    return null;
  }

  const elapsed = Date.now() - lastSeenMs;
  if (elapsed >= minIntervalMs) {
    return null;
  }

  return {
    retryAfterMs: Math.max(0, minIntervalMs - elapsed),
    lastSeenAt: existing?.last_seen_at ?? null,
  };
}

export async function recordSyncRequest(
  env: Env,
  request: SyncRequest,
): Promise<{
  currentVersion: number;
  payload: SyncPayload | null;
  pushed: boolean;
  downstreamStatus: string;
  responseCode: number | null;
}> {
  const currentVersion = await getCurrentVersion(env.DB);
  const seenAt = nowIso();
  const clientName = request.clientName?.trim() || null;
  const currentClientVersion = Math.max(0, Number(request.currentVersion ?? 0));

  await env.DB.prepare(
    `
      INSERT INTO downstream_clients (
        entry_kind,
        client_name,
        callback_url,
        client_version,
        last_sync_version,
        last_seen_at,
        last_status
      )
      VALUES ('sync-client', ?, ?, ?, 0, ?, 'pending')
      ON CONFLICT(callback_url) DO UPDATE SET
        entry_kind = 'sync-client',
        client_name = excluded.client_name,
        client_version = excluded.client_version,
        last_seen_at = excluded.last_seen_at,
        service_url = NULL,
        databack_version = NULL,
        report_count = 0,
        reported_at = NULL,
        payload_json = NULL
    `,
  )
    .bind(
      clientName,
      request.callbackUrl,
      currentClientVersion,
      seenAt,
    )
    .run();

  if (currentClientVersion >= currentVersion) {
    await env.DB.prepare(
      `
        UPDATE downstream_clients
        SET last_status = 'up-to-date',
            last_error = NULL,
            last_response_code = 204
        WHERE callback_url = ?
      `,
    )
      .bind(request.callbackUrl)
      .run();

    return {
      currentVersion,
      payload: null,
      pushed: false,
      downstreamStatus: 'up-to-date',
      responseCode: 204,
    };
  }

  const mode = request.mode === 'delta' ? 'delta' : 'full';
  const payload = await getPublishedPayload(
    env.DB,
    currentVersion,
    currentClientVersion,
    mode,
  );

  let responseCode: number | null = null;
  let downstreamStatus = 'queued';
  let pushed = false;
  let lastError: string | null = null;

  try {
    const response = await fetch(request.callbackUrl, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify(payload),
    });

    responseCode = response.status;
    pushed = response.ok;
    downstreamStatus = response.ok ? 'pushed' : 'push-failed';
    if (!response.ok) {
      lastError = await response.text();
    }
  } catch (error) {
    pushed = false;
    downstreamStatus = 'push-error';
    responseCode = null;
    lastError = error instanceof Error ? error.message : 'Unknown sync error';
  }

  await env.DB.prepare(
    `
      UPDATE downstream_clients
      SET last_sync_version = ?,
          last_push_at = ?,
          last_status = ?,
          last_response_code = ?,
          last_error = ?
      WHERE callback_url = ?
    `,
  )
    .bind(
      pushed ? currentVersion : currentClientVersion,
      seenAt,
      downstreamStatus,
      responseCode,
      lastError,
      request.callbackUrl,
    )
    .run();

  return {
    currentVersion,
    payload,
    pushed,
    downstreamStatus,
    responseCode,
  };
}

export async function recordSubReport(
  db: D1Database,
  report: SubReportPayload,
): Promise<DownstreamClient> {
  const receivedAt = nowIso();
  const storageKey = buildSubReportKey(report.serviceUrl);
  const persistedPayload: JsonObject = {
    service: report.service,
    serviceUrl: report.serviceUrl,
    databackVersion: report.databackVersion,
    reportCount: report.reportCount,
    reportedAt: report.reportedAt,
  };
  const payloadJson = stableStringify(persistedPayload);

  await db.prepare(
    `
      INSERT INTO downstream_clients (
        entry_kind,
        client_name,
        callback_url,
        client_version,
        last_sync_version,
        last_seen_at,
        last_push_at,
        last_status,
        last_response_code,
        last_error,
        service_url,
        databack_version,
        report_count,
        reported_at,
        payload_json
      )
      VALUES (
        'sub-report',
        ?, ?, ?, 0, ?, NULL, 'reported', 202, NULL, ?, ?, ?, ?, ?
      )
      ON CONFLICT(callback_url) DO UPDATE SET
        entry_kind = 'sub-report',
        client_name = excluded.client_name,
        client_version = excluded.client_version,
        last_seen_at = excluded.last_seen_at,
        service_url = excluded.service_url,
        databack_version = excluded.databack_version,
        report_count = excluded.report_count,
        reported_at = excluded.reported_at,
        payload_json = excluded.payload_json
    `,
  )
    .bind(
      report.service,
      storageKey,
      Math.max(0, Number(report.databackVersion ?? 0)),
      receivedAt,
      report.serviceUrl,
      report.databackVersion,
      report.reportCount,
      report.reportedAt,
      payloadJson,
    )
    .run();

  const storedRow = await db.prepare(
    `
      SELECT *
      FROM downstream_clients
      WHERE callback_url = ?
    `,
  )
    .bind(storageKey)
    .first<DownstreamClientRow>();

  if (!storedRow) {
    throw new Error('Failed to read back sub report record.');
  }

  return mapDownstreamClient(storedRow);
}

export async function pushSecureRecordsToRegisteredSubs(
  env: Env,
): Promise<
  Array<{
    serviceUrl: string;
    pushUrl: string;
    previousVersion: number;
    currentVersion: number;
    pushed: boolean;
    status: string;
    responseCode: number | null;
    totalRecords: number;
    lastPushAt: string | null;
    reason?: string;
  }>
> {
  const currentVersion = await getCurrentVersion(env.DB);
  const subServiceResult = await env.DB.prepare(
    `
      SELECT *
      FROM downstream_clients
      WHERE entry_kind = 'sub-report'
        AND service_url IS NOT NULL
      ORDER BY COALESCE(databack_version, 0) DESC, last_seen_at DESC
    `,
  ).all<DownstreamClientRow>();

  const rows = subServiceResult.results ?? [];
  const results: Array<{
    serviceUrl: string;
    pushUrl: string;
    previousVersion: number;
    currentVersion: number;
    pushed: boolean;
    status: string;
    responseCode: number | null;
    totalRecords: number;
    lastPushAt: string | null;
    reason?: string;
  }> = [];

  for (const row of rows) {
    const serviceUrl = row.service_url?.trim();
    if (!serviceUrl) {
      continue;
    }

    const pushUrl = buildSubPushUrl(serviceUrl);
    const previousVersion = readPushedMainVersion(row);

    if (previousVersion >= currentVersion) {
      await env.DB.prepare(
        `
          UPDATE downstream_clients
          SET last_status = 'up-to-date',
              last_response_code = 204,
              last_error = NULL
          WHERE id = ?
        `,
      )
        .bind(row.id)
        .run();

      results.push({
        serviceUrl,
        pushUrl,
        previousVersion,
        currentVersion,
        pushed: false,
        status: 'up-to-date',
        responseCode: 204,
        totalRecords: 0,
        lastPushAt: row.last_push_at,
      });
      continue;
    }

    const payload = await buildSubPushPayload(
      env.DB,
      currentVersion,
      previousVersion,
    );

    try {
      const formData = createJsonAttachmentFormData(
        `secure-records-v${currentVersion}.json`,
        payload as unknown as Record<string, unknown>,
      );
      const response = await fetch(pushUrl, {
        method: 'POST',
        headers: {
          ...(env.SUB_PUSH_TOKEN
            ? {
                authorization: `Bearer ${env.SUB_PUSH_TOKEN}`,
              }
            : {}),
        },
        body: formData,
      });

      if (response.ok) {
        const pushedAt = nowIso();
        await env.DB.prepare(
          `
            UPDATE downstream_clients
            SET last_sync_version = ?,
                last_push_at = ?,
                last_status = 'pushed',
                last_response_code = ?,
                last_error = NULL
            WHERE id = ?
          `,
        )
          .bind(
            currentVersion,
            pushedAt,
            response.status,
            row.id,
          )
          .run();

        results.push({
          serviceUrl,
          pushUrl,
          previousVersion,
          currentVersion,
          pushed: true,
          status: 'pushed',
          responseCode: response.status,
          totalRecords: payload.totalRecords,
          lastPushAt: pushedAt,
        });
        continue;
      }

      const errorText = await response.text();
      await env.DB.prepare(
        `
          UPDATE downstream_clients
          SET last_status = 'push-failed',
              last_response_code = ?,
              last_error = ?
          WHERE id = ?
        `,
      )
        .bind(
          response.status,
          errorText || `Push failed with status ${response.status}.`,
          row.id,
        )
        .run();

      results.push({
        serviceUrl,
        pushUrl,
        previousVersion,
        currentVersion,
        pushed: false,
        status: 'push-failed',
        responseCode: response.status,
        totalRecords: payload.totalRecords,
        lastPushAt: row.last_push_at,
        reason: errorText || `Push failed with status ${response.status}.`,
      });
    } catch (error) {
      const reason =
        error instanceof Error
          ? error.message
          : 'Unknown push error';

      await env.DB.prepare(
        `
          UPDATE downstream_clients
          SET last_status = 'push-error',
              last_response_code = NULL,
              last_error = ?
          WHERE id = ?
        `,
      )
        .bind(reason, row.id)
        .run();

      results.push({
        serviceUrl,
        pushUrl,
        previousVersion,
        currentVersion,
        pushed: false,
        status: 'push-error',
        responseCode: null,
        totalRecords: payload.totalRecords,
        lastPushAt: row.last_push_at,
        reason,
      });
    }
  }

  return results;
}

export async function pullDatabackFromRegisteredSubs(
  env: Env,
): Promise<
  Array<{
    serviceUrl: string;
    exportUrl: string;
    previousVersion: number;
    reportedVersion: number;
    pulled: boolean;
    status: string;
    responseCode: number | null;
    receivedCount: number;
    updatedCount: number;
    skippedCount: number;
    highestPulledVersion: number;
    lastPullAt: string | null;
    reason?: string;
  }>
> {
  const batchSize = Math.max(
    1,
    Math.min(Number(env.SUB_PULL_BATCH_SIZE ?? '10'), 100),
  );
  const recordLimit = Math.max(
    1,
    Math.min(Number(env.SUB_PULL_RECORD_LIMIT ?? '100'), 500),
  );
  const timeoutMs = Math.max(
    1000,
    Number(env.SUB_PULL_TIMEOUT_MS ?? '10000'),
  );
  const subServiceResult = await env.DB.prepare(
    `
      SELECT *
      FROM downstream_clients
      WHERE entry_kind = 'sub-report'
        AND service_url IS NOT NULL
      ORDER BY COALESCE(databack_version, 0) DESC, last_seen_at DESC
      LIMIT ?
    `,
  )
    .bind(batchSize)
    .all<DownstreamClientRow>();
  const rows = subServiceResult.results ?? [];
  const results: Array<{
    serviceUrl: string;
    exportUrl: string;
    previousVersion: number;
    reportedVersion: number;
    pulled: boolean;
    status: string;
    responseCode: number | null;
    receivedCount: number;
    updatedCount: number;
    skippedCount: number;
    highestPulledVersion: number;
    lastPullAt: string | null;
    reason?: string;
  }> = [];

  for (const row of rows) {
    const serviceUrl = row.service_url?.trim();
    if (!serviceUrl) {
      continue;
    }

    const exportUrl = buildSubExportUrl(serviceUrl);
    const previousVersion = readSubPullVersion(row);
    const reportedVersion = readReportedSubVersion(row);

    if (reportedVersion <= previousVersion) {
      const checkedAt = nowIso();
      await env.DB.prepare(
        `
          UPDATE downstream_clients
          SET last_pull_at = ?,
              last_pull_status = 'up-to-date',
              last_pull_response_code = 204,
              last_pull_error = NULL
          WHERE id = ?
        `,
      )
        .bind(checkedAt, row.id)
        .run();

      results.push({
        serviceUrl,
        exportUrl,
        previousVersion,
        reportedVersion,
        pulled: false,
        status: 'up-to-date',
        responseCode: 204,
        receivedCount: 0,
        updatedCount: 0,
        skippedCount: 0,
        highestPulledVersion: previousVersion,
        lastPullAt: checkedAt,
      });
      continue;
    }

    let highestPulledVersion = previousVersion;
    let receivedCount = 0;
    let updatedCount = 0;
    let skippedCount = 0;
    let responseCode: number | null = null;
    let lastPullAt: string | null = null;
    let failed = false;

    try {
      let hasMore = true;

      while (hasMore) {
        const requestUrl = new URL(exportUrl);
        requestUrl.searchParams.set(
          'afterVersion',
          String(highestPulledVersion),
        );
        requestUrl.searchParams.set(
          'limit',
          String(recordLimit),
        );

        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        try {
          const response = await fetch(requestUrl.toString(), {
            method: 'GET',
            headers: {
              ...(env.SUB_PUSH_TOKEN
                ? {
                    authorization: `Bearer ${env.SUB_PUSH_TOKEN}`,
                  }
                : {}),
            },
            signal: controller.signal,
          });
          responseCode = response.status;

          if (!response.ok) {
            const reason = await response.text();
            await env.DB.prepare(
              `
                UPDATE downstream_clients
                SET last_pull_status = 'pull-failed',
                    last_pull_response_code = ?,
                    last_pull_error = ?
                WHERE id = ?
              `,
            )
              .bind(
                response.status,
                reason || `Pull failed with status ${response.status}.`,
                row.id,
              )
              .run();

            results.push({
              serviceUrl,
              exportUrl,
              previousVersion,
              reportedVersion,
              pulled: false,
              status: 'pull-failed',
              responseCode: response.status,
              receivedCount,
              updatedCount,
              skippedCount,
              highestPulledVersion,
              lastPullAt: row.last_pull_at,
              reason: reason || `Pull failed with status ${response.status}.`,
            });
            failed = true;
            hasMore = false;
            break;
          }

          const exportFile = parseSubDatabackExportFile(
            JSON.parse(await response.text()),
          );
          const importResult = await importSubDatabackExportFile(
            env,
            serviceUrl,
            exportFile,
          );

          receivedCount += importResult.receivedCount;
          updatedCount += importResult.updatedCount;
          skippedCount += importResult.skippedCount;
          highestPulledVersion = Math.max(
            highestPulledVersion,
            importResult.highestPulledVersion,
            Number(exportFile.currentVersion ?? 0),
          );
          hasMore =
            exportFile.records.length >= recordLimit
            && highestPulledVersion < Number(exportFile.currentVersion ?? 0);
        } finally {
          clearTimeout(timeout);
        }
      }

      if (failed || responseCode === null) {
        continue;
      }

      lastPullAt = nowIso();
      const finalStatus =
        updatedCount > 0 || receivedCount > 0
          ? 'pulled'
          : 'up-to-date';

      await env.DB.prepare(
        `
          UPDATE downstream_clients
          SET last_pull_version = ?,
              last_pull_at = ?,
              last_pull_status = ?,
              last_pull_response_code = ?,
              last_pull_error = NULL
          WHERE id = ?
        `,
      )
        .bind(
          highestPulledVersion,
          lastPullAt,
          finalStatus,
          responseCode ?? 200,
          row.id,
        )
        .run();

      results.push({
        serviceUrl,
        exportUrl,
        previousVersion,
        reportedVersion,
        pulled: updatedCount > 0 || receivedCount > 0,
        status: finalStatus,
        responseCode,
        receivedCount,
        updatedCount,
        skippedCount,
        highestPulledVersion,
        lastPullAt,
      });
    } catch (error) {
      const reason =
        error instanceof Error
          ? error.message
          : 'Unknown pull error';

      await env.DB.prepare(
        `
          UPDATE downstream_clients
          SET last_pull_status = 'pull-error',
              last_pull_response_code = NULL,
              last_pull_error = ?
          WHERE id = ?
        `,
      )
        .bind(reason, row.id)
        .run();

      results.push({
        serviceUrl,
        exportUrl,
        previousVersion,
        reportedVersion,
        pulled: false,
        status: 'pull-error',
        responseCode: null,
        receivedCount,
        updatedCount,
        skippedCount,
        highestPulledVersion,
        lastPullAt: row.last_pull_at,
        reason,
      });
    }
  }

  return results;
}

export async function getOverview(
  db: D1Database,
): Promise<AnalyticsOverview> {
  const [totalsRow, rawBySource, syncStatuses, versionHistory] =
    await Promise.all([
      db
        .prepare(
          `
            SELECT
              (SELECT COUNT(*) FROM raw_records) AS rawRecords,
              (SELECT COUNT(*) FROM secure_records) AS secureRecords,
              (SELECT COUNT(*) FROM downstream_clients) AS downstreamClients,
              (SELECT COALESCE(MAX(version), 0) FROM secure_records) AS currentVersion
          `,
        )
        .first<{
          rawRecords: number;
          secureRecords: number;
          downstreamClients: number;
          currentVersion: number;
        }>(),
      db
        .prepare(
          `
            SELECT source, COUNT(*) AS count
            FROM raw_records
            GROUP BY source
            ORDER BY count DESC, source ASC
          `,
        )
        .all<{ source: string; count: number }>(),
      db
        .prepare(
          `
            SELECT last_status AS status, COUNT(*) AS count
            FROM downstream_clients
            GROUP BY last_status
            ORDER BY count DESC, status ASC
          `,
        )
        .all<{ status: string; count: number }>(),
      db
        .prepare(
          `
            SELECT record_key AS recordKey, version
            FROM secure_records
            ORDER BY version DESC
            LIMIT 20
          `,
        )
        .all<{ recordKey: string; version: number }>(),
    ]);

  return {
    totals: {
      rawRecords: Number(totalsRow?.rawRecords ?? 0),
      secureRecords: Number(totalsRow?.secureRecords ?? 0),
      downstreamClients: Number(totalsRow?.downstreamClients ?? 0),
      currentVersion: Number(totalsRow?.currentVersion ?? 0),
    },
    rawBySource: rawBySource.results ?? [],
    syncStatuses: syncStatuses.results ?? [],
    versionHistory: versionHistory.results ?? [],
  };
}

export async function getPublicDataset(
  db: D1Database,
): Promise<PublicDatasetResponse> {
  const [rawRecords, versionRow] = await Promise.all([
    listRawRecords(db),
    db
      .prepare(
        'SELECT COALESCE(MAX(version), 0) AS version FROM secure_records',
      )
      .first<{ version: number | null }>(),
  ]);

  const provinceCounts = new Map<string, number>();
  let ageTotal = 0;
  let ageCount = 0;

  const data = rawRecords.map((record) => {
    const province = readStringField(record.payload, 'province').trim();
    if (province) {
      provinceCounts.set(
        province,
        (provinceCounts.get(province) ?? 0) + 1,
      );
    }

    const age = readNumberField(record.payload, 'age');
    if (age !== null) {
      ageTotal += age;
      ageCount += 1;
    }

    return mapPublicDatasetItem(record);
  });

  return {
    avg_age: ageCount ? Math.round(ageTotal / ageCount) : 0,
    last_synced: Number(versionRow?.version ?? 0),
    statistics: Array.from(provinceCounts.entries())
      .map(([province, count]) => ({
        province,
        count,
      }))
      .sort((left, right) => {
        if (right.count !== left.count) {
          return right.count - left.count;
        }

        return left.province.localeCompare(right.province, 'zh-CN');
      }),
    data,
  };
}

export async function getAdminSnapshot(
  db: D1Database,
  limits: {
    rawRecords?: number;
    secureRecords?: number;
    downstreamClients?: number;
  } = {
    rawRecords: 200,
    secureRecords: 200,
    downstreamClients: 200,
  },
): Promise<AdminSnapshot> {
  const [overview, rawRecords, secureRecords, downstreamClients] =
    await Promise.all([
      getOverview(db),
      listRawRecords(db, limits.rawRecords),
      listSecureRecords(db, limits.secureRecords),
      listDownstreamClients(db, limits.downstreamClients),
    ]);

  return {
    overview,
    rawRecords,
    secureRecords,
    downstreamClients,
  };
}
