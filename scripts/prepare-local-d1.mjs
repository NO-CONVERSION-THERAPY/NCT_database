import { mkdirSync } from 'node:fs';
import { spawn } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const projectRoot = resolve(scriptDirectory, '..');
const persistPath = resolve(projectRoot, '.wrangler/state');

mkdirSync(persistPath, { recursive: true });

console.log('Preparing local D1 debug database...');
console.log(`Persistence path: ${persistPath}`);

const child = spawn(
  process.platform === 'win32' ? 'npx.cmd' : 'npx',
  [
    'wrangler',
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
    stdio: 'inherit',
    env: {
      ...process.env,
      CI: process.env.CI ?? '1',
    },
  },
);

child.on('error', (error) => {
  console.error('Failed to start Wrangler for local D1 preparation.');
  console.error(error);
  process.exit(1);
});

child.on('close', (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }

  if (code !== 0) {
    process.exit(code ?? 1);
    return;
  }

  console.log('Local D1 debug database is ready.');
  process.exit(0);
});
