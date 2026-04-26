import type { JsonObject } from '../../shared/types';
import { sha256 } from './crypto';
import { stableStringify } from './json';

const contentFingerprintSchema = 'nct-record-content:v1';
const contentVersionIncrementMask = 0xffffff;

export async function computeRecordContentFingerprint(
  payload: JsonObject,
): Promise<string> {
  return sha256(
    stableStringify({
      payload,
      schema: contentFingerprintSchema,
    }),
  );
}

export function deriveRecordContentVersion(
  previousVersion: number,
  fingerprint: string,
): number {
  const normalizedPreviousVersion = Number.isFinite(previousVersion)
    ? Math.max(0, Math.trunc(previousVersion))
    : 0;
  const seed = Number.parseInt(fingerprint.slice(0, 6), 16);

  if (!Number.isFinite(seed)) {
    throw new Error('Record content fingerprint must start with hexadecimal characters.');
  }

  const increment = (seed & contentVersionIncrementMask) + 1;
  const nextVersion = normalizedPreviousVersion + increment;

  if (!Number.isSafeInteger(nextVersion)) {
    throw new Error('Record content version exceeds the JavaScript safe integer range.');
  }

  return nextVersion;
}
