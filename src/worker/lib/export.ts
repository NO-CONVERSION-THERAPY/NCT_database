import JSZip from 'jszip';
import type { AdminSnapshot } from '../../shared/types';
import { getAdminSnapshot } from './data';

export function hasExportBucket(env: Env): env is Env & { EXPORT_BUCKET: R2Bucket } {
  return Boolean(env.EXPORT_BUCKET);
}

function arrayBufferToBase64(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  let binary = '';

  for (let index = 0; index < bytes.length; index += 0x8000) {
    const chunk = bytes.subarray(index, index + 0x8000);
    binary += String.fromCharCode(...chunk);
  }

  return btoa(binary);
}

function toCsv(
  rows: Array<Record<string, unknown>>,
): string {
  if (!rows.length) {
    return '';
  }

  const headers = Array.from(
    rows.reduce<Set<string>>((accumulator, row) => {
      Object.keys(row).forEach((key) => accumulator.add(key));
      return accumulator;
    }, new Set<string>()),
  );

  const escapeCell = (value: unknown): string => {
    const serialized =
      value === null || value === undefined
        ? ''
        : typeof value === 'string'
          ? value
          : JSON.stringify(value);

    return `"${serialized.replaceAll('"', '""')}"`;
  };

  const lines = [
    headers.join(','),
    ...rows.map((row) =>
      headers.map((header) => escapeCell(row[header])).join(','),
    ),
  ];

  return lines.join('\n');
}

function flattenSnapshot(
  snapshot: AdminSnapshot,
): Record<string, unknown[]> {
  return {
    raw_records: snapshot.rawRecords.map((record) => ({
      ...record,
      payload: JSON.stringify(record.payload),
      payloadColumns: JSON.stringify(record.payloadColumns),
    })),
    secure_records: snapshot.secureRecords.map((record) => ({
      ...record,
      publicData: JSON.stringify(record.publicData),
      publicColumns: JSON.stringify(record.publicColumns),
      encryptedData: JSON.stringify(record.encryptedData),
      encryptedColumns: JSON.stringify(record.encryptedColumns),
      encryptFields: JSON.stringify(record.encryptFields),
    })),
    downstream_clients: snapshot.downstreamClients,
  };
}

async function createArchiveBuffer(
  snapshot: AdminSnapshot,
): Promise<ArrayBuffer> {
  const zip = new JSZip();
  const flattened = flattenSnapshot(snapshot);

  zip.file(
    'snapshot.json',
    JSON.stringify(snapshot, null, 2),
  );
  zip.file(
    'overview.json',
    JSON.stringify(snapshot.overview, null, 2),
  );

  Object.entries(flattened).forEach(([name, rows]) => {
    zip.file(`${name}.json`, JSON.stringify(rows, null, 2));
    zip.file(`${name}.csv`, toCsv(rows as Array<Record<string, unknown>>));
  });

  return zip.generateAsync({ type: 'arraybuffer' });
}

async function sendExportEmail(
  env: Env,
  fileName: string,
  archiveBuffer: ArrayBuffer,
): Promise<'skipped' | 'sent'> {
  if (
    !env.RESEND_API_KEY ||
    !env.EXPORT_EMAIL_TO ||
    !env.EXPORT_EMAIL_FROM
  ) {
    return 'skipped';
  }

  const base64Archive = arrayBufferToBase64(archiveBuffer);

  const response = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      authorization: `Bearer ${env.RESEND_API_KEY}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      from: env.EXPORT_EMAIL_FROM,
      to: [env.EXPORT_EMAIL_TO],
      subject: `${env.APP_NAME ?? 'NCT API SQL'} D1 export ${new Date().toISOString()}`,
      html: '<p>Attached is the latest D1 export package.</p>',
      attachments: [
        {
          filename: fileName,
          content: base64Archive,
        },
      ],
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Resend email failed: ${response.status} ${errorText}`);
  }

  return 'sent';
}

export async function exportSnapshot(
  env: Env,
): Promise<{
  fileName: string;
  objectKey: string;
  emailStatus: 'skipped' | 'sent';
}> {
  if (!hasExportBucket(env)) {
    throw new Error('EXPORT_BUCKET is not configured. Enable R2 and add the binding before running exports.');
  }

  const snapshot = await getAdminSnapshot(env.DB, {
    rawRecords: undefined,
    secureRecords: undefined,
    downstreamClients: undefined,
  });
  const generatedAt = new Date().toISOString();
  const fileName = `d1-export-${generatedAt.replaceAll(':', '-')}.zip`;
  const objectKey = `exports/${generatedAt.slice(0, 10)}/${fileName}`;
  const archiveBuffer = await createArchiveBuffer(snapshot);

  await env.EXPORT_BUCKET.put(objectKey, archiveBuffer, {
    httpMetadata: {
      contentType: 'application/zip',
      contentDisposition: `attachment; filename="${fileName}"`,
    },
    customMetadata: {
      generatedAt,
    },
  });

  const emailStatus = await sendExportEmail(env, fileName, archiveBuffer);

  return {
    fileName,
    objectKey,
    emailStatus,
  };
}
