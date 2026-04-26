import { mkdirSync, readdirSync } from 'node:fs';
import { spawn } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const projectRoot = resolve(scriptDirectory, '..');
const migrationDirectory = resolve(projectRoot, 'migrations');
const persistPath = resolve(projectRoot, '.wrangler/state');
const wranglerBinary = resolve(
  projectRoot,
  'node_modules',
  '.bin',
  process.platform === 'win32' ? 'wrangler.cmd' : 'wrangler',
);
const migrationFiles = readdirSync(migrationDirectory)
  .filter((name) => name.endsWith('.sql'))
  .sort();

mkdirSync(persistPath, { recursive: true });

console.log('Preparing local D1 debug database...');
console.log(`Persistence path: ${persistPath}`);

let outputText = '';
let finishing = false;

function stripAnsi(value) {
  return value.replace(/\x1B\[[0-?]*[ -/]*[@-~]/g, '');
}

function hasSuccessfulMigrationOutput() {
  const cleanOutput = stripAnsi(outputText);

  if (cleanOutput.includes('No migrations to apply!')) {
    return true;
  }

  return migrationFiles.length > 0
    && migrationFiles.every((name) => {
      const escapedName = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      return new RegExp(`${escapedName}\\s+│\\s+✅`).test(cleanOutput);
    });
}

function finishSuccessfully() {
  if (finishing) {
    return;
  }

  finishing = true;
  console.log('Local D1 debug database is ready.');

  if (!child.killed) {
    child.kill('SIGTERM');
  }

  setTimeout(() => {
    if (!child.killed) {
      child.kill('SIGKILL');
    }
    process.exit(0);
  }, 500).unref();
}

function handleOutput(chunk, stream) {
  const text = chunk.toString();
  outputText += text;
  stream.write(text);

  if (hasSuccessfulMigrationOutput()) {
    setTimeout(finishSuccessfully, 500).unref();
  }
}

const child = spawn(
  wranglerBinary,
  [
    'd1',
    'migrations',
    'apply',
    'DB',
    '--local',
    '--persist-to',
    persistPath,
  ],
  {
    cwd: projectRoot,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: {
      ...process.env,
      CI: process.env.CI ?? '1',
      WRANGLER_SEND_METRICS: process.env.WRANGLER_SEND_METRICS ?? 'false',
    },
  },
);

child.stdout.on('data', (chunk) => handleOutput(chunk, process.stdout));
child.stderr.on('data', (chunk) => handleOutput(chunk, process.stderr));

child.on('error', (error) => {
  console.error('Failed to start Wrangler for local D1 preparation.');
  console.error(error);
  process.exit(1);
});

child.on('close', (code, signal) => {
  if (finishing) {
    process.exit(0);
    return;
  }

  if (hasSuccessfulMigrationOutput()) {
    finishSuccessfully();
    return;
  }

  if (signal) {
    process.kill(process.pid, signal);
    return;
  }

  if (code !== 0) {
    process.exit(code ?? 1);
    return;
  }

  process.exit(0);
});
