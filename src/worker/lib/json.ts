import type { JsonObject, JsonValue } from '../../shared/types';

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    !Array.isArray(value)
  );
}

function normalizeJsonValue(value: unknown): JsonValue {
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  ) {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map((item) => normalizeJsonValue(item));
  }

  if (isPlainObject(value)) {
    return Object.keys(value)
      .sort()
      .reduce<JsonObject>((accumulator, key) => {
        accumulator[key] = normalizeJsonValue(value[key]);
        return accumulator;
      }, {});
  }

  return String(value);
}

export function toJsonObject(value: unknown): JsonObject {
  if (!isPlainObject(value)) {
    throw new Error('Payload must be a JSON object.');
  }

  return normalizeJsonValue(value) as JsonObject;
}

export function stableStringify(value: unknown): string {
  return JSON.stringify(normalizeJsonValue(value));
}

export function parseJsonObject(
  value: string,
): JsonObject {
  return toJsonObject(JSON.parse(value));
}

export function parseStringArray(
  value: string,
): string[] {
  const parsed = JSON.parse(value);
  if (!Array.isArray(parsed)) {
    return [];
  }

  return parsed
    .filter((item) => typeof item === 'string')
    .map((item) => item.trim())
    .filter(Boolean);
}
