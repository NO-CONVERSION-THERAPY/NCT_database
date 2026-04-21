import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { z } from 'zod';
import { toJsonObject } from './lib/json';
import {
  getAdminSnapshot,
  getSubReportThrottleState,
  getPublicDataset,
  getPublishedPayload,
  ingestRecords,
  isRecognizedSubService,
  pullDatabackFromRegisteredSubs,
  pushSecureRecordsToRegisteredSubs,
  rebuildSecureRecords,
  recordSubReport,
} from './lib/data';
import { exportSnapshot } from './lib/export';
import { assertToken } from './lib/security';

const ingestSchema = z.object({
  records: z
    .array(
      z.object({
        recordKey: z.string().optional(),
        source: z.string().optional(),
        encryptFields: z.array(z.string()).optional(),
        payload: z
          .record(z.string(), z.unknown())
          .transform((value) => toJsonObject(value)),
      }),
    )
    .min(1),
});

const subReportSchema = z.object({
  service: z.string().min(1),
  serviceUrl: z.string().url(),
  databackVersion: z.number().int().min(0).nullable(),
  reportCount: z.number().int().min(1),
  reportedAt: z.string().min(1),
});

const PUSH_CRON = '*/20 * * * *';
const EXPORT_CRON = '0 18 * * *';

const app = new Hono<{ Bindings: Env }>();

async function serveConsoleShell(context: {
  env: Env;
  req: {
    url: string;
  };
}) {
  const response = await context.env.ASSETS.fetch(
    new Request(new URL('/index.html', context.req.url)),
  );

  if (response.status !== 404) {
    return response;
  }

  return new Response(
    'Console assets not found. Build the client before deploying the Worker.',
    {
      status: 404,
      headers: {
        'content-type': 'text/plain; charset=utf-8',
      },
    },
  );
}

app.use(
  '/api/*',
  cors({
    origin: '*',
    allowHeaders: ['content-type', 'authorization', 'x-api-token'],
    allowMethods: ['GET', 'POST', 'OPTIONS'],
  }),
);

app.get('/', async (context) => {
  return context.json(await getPublicDataset(context.env.DB));
});

app.get('/Console', async (context) => {
  return serveConsoleShell(context);
});

app.get('/Console/*', async (context) => {
  return serveConsoleShell(context);
});

app.get('/assets/*', async (context) => {
  return context.env.ASSETS.fetch(context.req.raw);
});

app.get('/api/health', async (context) => {
  const snapshot = await getAdminSnapshot(context.env.DB);

  return context.json({
    status: 'ok',
    app: context.env.APP_NAME ?? 'NCT API SQL',
    currentVersion: snapshot.overview.totals.currentVersion,
    checkedAt: new Date().toISOString(),
  });
});

app.post('/api/ingest', async (context) => {
  const authError = assertToken(
    context,
    context.env.INGEST_TOKEN,
    'Ingest',
  );
  if (authError) {
    return authError;
  }

  const payload = await context.req.json();
  const parsed = ingestSchema.safeParse(payload);
  if (!parsed.success) {
    return context.json(
      {
        error: 'Invalid ingest payload.',
        details: parsed.error.flatten(),
      },
      400,
    );
  }

  const results = await ingestRecords(context.env, parsed.data.records);

  return context.json({
    message: 'Records ingested successfully.',
    updatedCount: results.filter((item) => item.updated).length,
    results,
  });
});

app.post('/api/sync', async (context) => {
  return context.json(
    {
      error:
        'Deprecated. nct-api-sql no longer accepts downstream sync requests. Registered nct-api-sql-sub services are pushed by cron every 20 minutes.',
    },
    410,
  );
});

app.post('/api/sub/report', async (context) => {
  const authError = assertToken(
    context,
    context.env.SUB_REPORT_TOKEN,
    'Sub report',
  );
  if (authError) {
    return authError;
  }

  const payload = await context.req.json();
  const parsed = subReportSchema.safeParse(payload);
  if (!parsed.success) {
    return context.json(
      {
        error: 'Invalid sub report payload.',
        details: parsed.error.flatten(),
      },
      400,
    );
  }

  if (!isRecognizedSubService(parsed.data.service)) {
    return context.json(
      {
        error: 'Only nct-api-sql-sub reports are accepted.',
      },
      403,
    );
  }

  const throttleState = await getSubReportThrottleState(
    context.env.DB,
    parsed.data.serviceUrl,
    Math.max(0, Number(context.env.SUB_REPORT_MIN_INTERVAL_MS ?? '5000')),
  );
  if (throttleState) {
    return context.json(
      {
        error: 'Sub report was throttled.',
        retryAfterMs: throttleState.retryAfterMs,
        lastSeenAt: throttleState.lastSeenAt,
      },
      429,
    );
  }

  const stored = await recordSubReport(context.env.DB, parsed.data);

  return context.json(
    {
      accepted: true,
      stored,
    },
    202,
  );
});

app.get('/api/public/secure-records', async (context) => {
  const authError = assertToken(
    context,
    context.env.SYNC_TOKEN,
    'Sync',
  );
  if (authError) {
    return authError;
  }

  const currentVersion = Number(
    context.req.query('currentVersion') ?? '0',
  );
  const mode = context.req.query('mode') === 'delta' ? 'delta' : 'full';

  const payload = await getPublishedPayload(
    context.env.DB,
    await context.env.DB.prepare(
      'SELECT COALESCE(MAX(version), 0) AS version FROM secure_records',
    ).first<{ version: number }>().then((row) => Number(row?.version ?? 0)),
    currentVersion,
    mode,
  );

  return context.json(payload);
});

app.get('/api/admin/snapshot', async (context) => {
  const authError = assertToken(
    context,
    context.env.ADMIN_TOKEN,
    'Admin',
  );
  if (authError) {
    return authError;
  }

  return context.json(await getAdminSnapshot(context.env.DB));
});

app.post('/api/admin/rebuild-secure', async (context) => {
  const authError = assertToken(
    context,
    context.env.ADMIN_TOKEN,
    'Admin',
  );
  if (authError) {
    return authError;
  }

  const results = await rebuildSecureRecords(context.env);

  return context.json({
    message: 'Secure table rebuilt from raw records.',
    processed: results.length,
    updated: results.filter((item) => item.updated).length,
    results,
  });
});

app.post('/api/admin/export-now', async (context) => {
  const authError = assertToken(
    context,
    context.env.ADMIN_TOKEN,
    'Admin',
  );
  if (authError) {
    return authError;
  }

  const result = await exportSnapshot(context.env);
  return context.json({
    message: 'Export completed.',
    ...result,
  });
});

app.post('/api/admin/push-now', async (context) => {
  const authError = assertToken(
    context,
    context.env.ADMIN_TOKEN,
    'Admin',
  );
  if (authError) {
    return authError;
  }

  const results = await pushSecureRecordsToRegisteredSubs(context.env);
  return context.json({
    message: 'Push run completed.',
    totalTargets: results.length,
    pushedTargets: results.filter((item) => item.pushed).length,
    results,
  });
});

app.post('/api/admin/pull-now', async (context) => {
  const authError = assertToken(
    context,
    context.env.ADMIN_TOKEN,
    'Admin',
  );
  if (authError) {
    return authError;
  }

  const results = await pullDatabackFromRegisteredSubs(context.env);
  return context.json({
    message: 'Pull run completed.',
    totalTargets: results.length,
    pulledTargets: results.filter((item) => item.pulled).length,
    results,
  });
});

app.notFound(async (context) => {
  if (context.req.path.startsWith('/api/')) {
    return context.json(
      {
        error: 'Not found.',
      },
      404,
    );
  }

  const assetResponse = await context.env.ASSETS.fetch(context.req.raw);
  if (assetResponse.status !== 404) {
    return assetResponse;
  }

  return context.json(
    {
      error: 'Not found.',
    },
    404,
  );
});

export default {
  fetch(request: Request, env: Env, executionCtx: ExecutionContext) {
    return app.fetch(request, env, executionCtx);
  },
  scheduled(
    controller: ScheduledController,
    env: Env,
    executionCtx: ExecutionContext,
  ) {
    if (controller.cron === PUSH_CRON) {
      executionCtx.waitUntil(
        (async () => {
          await pushSecureRecordsToRegisteredSubs(env);
          await pullDatabackFromRegisteredSubs(env);
        })(),
      );
      return;
    }

    if (controller.cron === EXPORT_CRON) {
      executionCtx.waitUntil(exportSnapshot(env));
    }
  },
};
