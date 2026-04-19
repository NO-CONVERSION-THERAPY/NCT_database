import type { JsonValue } from '../../shared/types';
import { stableStringify } from './json';

export type DynamicColumnKind =
  | 'payload'
  | 'public'
  | 'encrypted';

const KIND_PREFIX: Record<DynamicColumnKind, string> = {
  payload: 'payload',
  public: 'public',
  encrypted: 'encrypted',
};

type TableInfoRow = {
  name: string;
};

function hashFieldName(value: string): string {
  let hash = 0x811c9dc5;

  for (const character of value) {
    hash ^= character.codePointAt(0) ?? 0;
    hash = Math.imul(hash, 0x01000193);
  }

  return (hash >>> 0).toString(36);
}

function normalizeFieldName(
  fieldName: string,
): string {
  const normalized = fieldName
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_');

  const safeName = normalized || 'field';
  const prefixed = /^[0-9]/.test(safeName)
    ? `f_${safeName}`
    : safeName;

  return prefixed.slice(0, 24);
}

function quoteIdentifier(
  identifier: string,
): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function dynamicColumnName(
  kind: DynamicColumnKind,
  fieldName: string,
): string {
  const baseName = normalizeFieldName(fieldName);
  const hash = hashFieldName(fieldName).slice(0, 6);
  return `${KIND_PREFIX[kind]}_${baseName}_${hash}`;
}

export async function listTableColumns(
  db: D1Database,
  tableName: string,
): Promise<Set<string>> {
  const result = await db
    .prepare(`PRAGMA table_info(${quoteIdentifier(tableName)})`)
    .all<TableInfoRow>();

  return new Set(
    (result.results ?? []).map((row) => row.name),
  );
}

export async function ensureDynamicColumns(
  db: D1Database,
  tableName: string,
  kind: DynamicColumnKind,
  fieldNames: Iterable<string>,
): Promise<Map<string, string>> {
  const uniqueFieldNames = Array.from(
    new Set(
      Array.from(fieldNames)
        .map((fieldName) => fieldName.trim())
        .filter(Boolean),
    ),
  ).sort((left, right) => left.localeCompare(right));

  const mappings = new Map<string, string>();
  if (!uniqueFieldNames.length) {
    return mappings;
  }

  const existingColumns = await listTableColumns(db, tableName);

  for (const fieldName of uniqueFieldNames) {
    const columnName = dynamicColumnName(kind, fieldName);
    mappings.set(fieldName, columnName);

    if (existingColumns.has(columnName)) {
      continue;
    }

    try {
      await db
        .prepare(
          `
            ALTER TABLE ${quoteIdentifier(tableName)}
            ADD COLUMN ${quoteIdentifier(columnName)} TEXT
          `,
        )
        .run();
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : String(error);

      if (!/duplicate column name/i.test(message)) {
        throw error;
      }
    }

    existingColumns.add(columnName);
  }

  return mappings;
}

export function serializeDynamicColumnValue(
  value: JsonValue,
): string | null {
  if (value === null) {
    return null;
  }

  if (typeof value === 'string') {
    return value;
  }

  if (
    typeof value === 'number' ||
    typeof value === 'boolean'
  ) {
    return String(value);
  }

  return stableStringify(value);
}

export function extractDynamicColumns(
  row: Record<string, unknown>,
  kind: DynamicColumnKind,
  fieldNames: Iterable<string>,
): Record<string, string | null> {
  const values: Record<string, string | null> = {};

  Array.from(
    new Set(Array.from(fieldNames).filter(Boolean)),
  )
    .sort((left, right) => left.localeCompare(right))
    .forEach((fieldName) => {
      const columnName = dynamicColumnName(kind, fieldName);
      const value = row[columnName];

      if (value === undefined) {
        return;
      }

      values[fieldName] =
        value === null ? null : String(value);
    });

  return values;
}
