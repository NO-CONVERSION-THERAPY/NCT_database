import type { EncryptedEnvelope, JsonObject } from '../../shared/types';
import { stableStringify } from './json';

const encoder = new TextEncoder();

function toArrayBuffer(view: Uint8Array): ArrayBuffer {
  return view.buffer.slice(
    view.byteOffset,
    view.byteOffset + view.byteLength,
  ) as ArrayBuffer;
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = '';
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

function base64ToBytes(value: string): Uint8Array {
  const binary = atob(value);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}

async function importAesKey(secret: string): Promise<CryptoKey> {
  const rawKey = base64ToBytes(secret);
  if (rawKey.byteLength !== 32) {
    throw new Error('ENCRYPTION_KEY must be a base64-encoded 32-byte value.');
  }

  return crypto.subtle.importKey(
    'raw',
    toArrayBuffer(rawKey),
    { name: 'AES-GCM' },
    false,
    ['encrypt'],
  );
}

export async function sha256(
  value: string,
): Promise<string> {
  const digest = await crypto.subtle.digest(
    'SHA-256',
    toArrayBuffer(encoder.encode(value)),
  );

  return Array.from(new Uint8Array(digest))
    .map((chunk) => chunk.toString(16).padStart(2, '0'))
    .join('');
}

export async function encryptObject(
  payload: JsonObject,
  secret: string,
): Promise<EncryptedEnvelope> {
  const key = await importAesKey(secret);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const plaintext = encoder.encode(stableStringify(payload));
  const ciphertext = await crypto.subtle.encrypt(
    {
      name: 'AES-GCM',
      iv: toArrayBuffer(iv),
    },
    key,
    toArrayBuffer(plaintext),
  );

  return {
    algorithm: 'AES-GCM',
    iv: bytesToBase64(iv),
    ciphertext: bytesToBase64(new Uint8Array(ciphertext)),
  };
}
