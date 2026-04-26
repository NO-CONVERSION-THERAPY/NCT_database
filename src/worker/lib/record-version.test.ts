import { describe, expect, it } from 'vitest';
import {
  computeRecordContentFingerprint,
  deriveRecordContentVersion,
} from './record-version';

describe('record content version helpers', () => {
  it('derives stable fingerprints and monotonic versions from canonical payload content', async () => {
    const left = await computeRecordContentFingerprint({
      zebra: true,
      alpha: {
        second: 2,
        first: 1,
      },
    });
    const right = await computeRecordContentFingerprint({
      alpha: {
        first: 1,
        second: 2,
      },
      zebra: true,
    });

    expect(left).toBe(
      '4fe91b172e25405b593ee505e764f7959eadbdd5b50af389eafdcea84a92460a',
    );
    expect(right).toBe(left);
    expect(deriveRecordContentVersion(100, left)).toBe(5237120);
  });
});
