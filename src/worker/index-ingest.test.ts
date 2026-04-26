import { afterEach, describe, expect, it, vi } from 'vitest';

const {
  ingestRecordsMock,
  pushSecureRecordsToRegisteredSubsMock,
} = vi.hoisted(() => ({
  ingestRecordsMock: vi.fn(),
  pushSecureRecordsToRegisteredSubsMock: vi.fn(),
}));

vi.mock('./lib/data', () => ({
  NCT_SUB_SERVICE_WATERMARK: 'nct-api-sql-sub:v1',
  bootstrapSubServiceAuth: vi.fn(),
  getAdminSnapshot: vi.fn(),
  getPublicDataset: vi.fn(),
  getPublishedPayload: vi.fn(),
  getSubReportThrottleState: vi.fn(),
  ingestRecords: ingestRecordsMock,
  ingestSubFormRecords: vi.fn(),
  isRecognizedSubService: vi.fn(),
  pullDatabackFromRegisteredSubs: vi.fn(),
  pushSecureRecordsToRegisteredSubs: pushSecureRecordsToRegisteredSubsMock,
  rebuildSecureRecords: vi.fn(),
  recordSubReport: vi.fn(),
  verifySubServiceToken: vi.fn(),
}));

import worker from './index';

afterEach(() => {
  vi.clearAllMocks();
});

describe('/api/ingest', () => {
  it('schedules secure record communication when raw ingest updates data', async () => {
    const pushPromise = Promise.resolve([
      {
        currentVersion: 8,
        lastPushAt: '2026-04-24T00:00:00.000Z',
        previousVersion: 0,
        pushed: true,
        pushUrl: 'https://sub.example.com/api/push/secure-records',
        responseCode: 202,
        serviceUrl: 'https://sub.example.com',
        status: 'pushed',
        totalRecords: 1,
      },
    ]);
    const env = {
      INGEST_TOKEN: 'ingest-secret',
    } as Env;
    const executionCtx = {
      passThroughOnException: vi.fn(),
      waitUntil: vi.fn(),
    } as unknown as ExecutionContext;

    ingestRecordsMock.mockResolvedValue([
      {
        fingerprint: 'fingerprint-1',
        rawRecordId: 'raw-1',
        recordKey: 'form:updated-record',
        secureRecordId: 'secure-1',
        updated: true,
        version: 8,
      },
    ]);
    pushSecureRecordsToRegisteredSubsMock.mockReturnValue(pushPromise);

    const response = await worker.fetch(
      new Request('https://mother.example.com/api/ingest', {
        body: JSON.stringify({
          records: [
            {
              payload: {
                contact: '13900000000',
                name: '测试受害者',
              },
              recordKey: 'form:updated-record',
            },
          ],
        }),
        headers: {
          authorization: 'Bearer ingest-secret',
          'content-type': 'application/json',
        },
        method: 'POST',
      }),
      env,
      executionCtx,
    );
    const body = await response.json() as {
      updatedCount: number;
    };

    expect(response.status).toBe(200);
    expect(body.updatedCount).toBe(1);
    expect(ingestRecordsMock).toHaveBeenCalledWith(env, [
      {
        payload: {
          contact: '13900000000',
          name: '测试受害者',
        },
        recordKey: 'form:updated-record',
      },
    ]);
    expect(pushSecureRecordsToRegisteredSubsMock).toHaveBeenCalledWith(env);
    expect(executionCtx.waitUntil).toHaveBeenCalledWith(pushPromise);
  });
});
