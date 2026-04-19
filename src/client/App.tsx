import { Suspense, lazy, useEffect, useState } from 'react';
import type { AdminSnapshot } from '../shared/types';
import { apiRequest } from './api';

const AnalyticsSection = lazy(() => import('./AnalyticsSection'));

const STORAGE_KEYS = {
  admin: 'nct-api-sql-admin-token',
  ingest: 'nct-api-sql-ingest-token',
  sync: 'nct-api-sql-sync-token',
} as const;

const sampleIngestPayload = JSON.stringify(
  {
    records: [
      {
        recordKey: 'patient-1001',
        source: 'hospital-a',
        encryptFields: ['name', 'phone', 'email'],
        payload: {
          id: 'patient-1001',
          name: 'Zhang San',
          phone: '13800000000',
          email: 'demo@example.com',
          city: 'Shanghai',
          score: 91,
          category: 'A',
        },
      },
    ],
  },
  null,
  2,
);

function truncate(
  value: string,
  limit = 72,
): string {
  if (value.length <= limit) {
    return value;
  }

  return `${value.slice(0, limit)}...`;
}

function toPrettyJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function MetricCard(props: {
  label: string;
  value: number | string;
  helper: string;
}) {
  return (
    <article className="metric-card glass-panel">
      <span className="metric-label">{props.label}</span>
      <strong className="metric-value">{props.value}</strong>
      <p className="metric-helper">{props.helper}</p>
    </article>
  );
}

function SectionTitle(props: {
  eyebrow: string;
  title: string;
  description: string;
}) {
  return (
    <div className="section-title">
      <span className="eyebrow">{props.eyebrow}</span>
      <h2>{props.title}</h2>
      <p>{props.description}</p>
    </div>
  );
}

function TableBlock(props: {
  title: string;
  columns: string[];
  rows: Array<string[]>;
}) {
  return (
    <section className="glass-panel table-panel">
      <div className="table-heading">
        <h3>{props.title}</h3>
        <span>{props.rows.length} rows</span>
      </div>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              {props.columns.map((column) => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {props.rows.length ? (
              props.rows.map((row, rowIndex) => (
                <tr key={`${props.title}-${rowIndex}`}>
                  {row.map((cell, cellIndex) => (
                    <td key={`${rowIndex}-${cellIndex}`}>
                      <span title={cell}>{cell}</span>
                    </td>
                  ))}
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={props.columns.length} className="empty-cell">
                  No rows yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export default function App() {
  const [snapshot, setSnapshot] = useState<AdminSnapshot | null>(null);
  const [loading, setLoading] = useState(true);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [message, setMessage] = useState<string>('Console ready.');
  const [error, setError] = useState<string | null>(null);
  const [adminToken, setAdminToken] = useState(
    () => localStorage.getItem(STORAGE_KEYS.admin) ?? '',
  );
  const [ingestToken, setIngestToken] = useState(
    () => localStorage.getItem(STORAGE_KEYS.ingest) ?? '',
  );
  const [syncToken, setSyncToken] = useState(
    () => localStorage.getItem(STORAGE_KEYS.sync) ?? '',
  );
  const [ingestPayload, setIngestPayload] = useState(sampleIngestPayload);
  const [syncClientName, setSyncClientName] = useState('demo-consumer');
  const [syncCallbackUrl, setSyncCallbackUrl] = useState(
    'https://example-downstream.com/sync',
  );
  const [syncVersion, setSyncVersion] = useState(0);
  const [syncMode, setSyncMode] = useState<'full' | 'delta'>('full');

  async function loadSnapshot() {
    setLoading(true);
    setError(null);

    try {
      const nextSnapshot = await apiRequest<AdminSnapshot>(
        '/api/admin/snapshot',
        {
          token: adminToken || undefined,
        },
      );
      setSnapshot(nextSnapshot);
      setSyncVersion(nextSnapshot.overview.totals.currentVersion);
    } catch (loadError) {
      const nextError =
        loadError instanceof Error ? loadError.message : 'Failed to load data.';
      setError(nextError);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    localStorage.setItem(STORAGE_KEYS.admin, adminToken);
  }, [adminToken]);

  useEffect(() => {
    localStorage.setItem(STORAGE_KEYS.ingest, ingestToken);
  }, [ingestToken]);

  useEffect(() => {
    localStorage.setItem(STORAGE_KEYS.sync, syncToken);
  }, [syncToken]);

  useEffect(() => {
    void loadSnapshot();
  }, []);

  async function runAction(
    actionName: string,
    task: () => Promise<void>,
  ) {
    setBusyAction(actionName);
    setError(null);

    try {
      await task();
    } catch (taskError) {
      const nextError =
        taskError instanceof Error ? taskError.message : 'Action failed.';
      setError(nextError);
      setMessage(`${actionName} failed.`);
    } finally {
      setBusyAction(null);
    }
  }

  async function handleIngestSubmit(
    event: React.FormEvent<HTMLFormElement>,
  ) {
    event.preventDefault();
    await runAction('Ingest', async () => {
      const parsedBody = JSON.parse(ingestPayload);
      const response = await apiRequest<{
        updatedCount: number;
      }>('/api/ingest', {
        method: 'POST',
        token: ingestToken || adminToken || undefined,
        body: parsedBody,
      });
      setMessage(`Ingest completed. ${response.updatedCount} record(s) changed.`);
      await loadSnapshot();
    });
  }

  async function handleSyncSubmit(
    event: React.FormEvent<HTMLFormElement>,
  ) {
    event.preventDefault();
    await runAction('Sync', async () => {
      const response = await apiRequest<{
        currentVersion: number;
        pushed: boolean;
        downstreamStatus: string;
      }>('/api/sync', {
        method: 'POST',
        token: syncToken || adminToken || undefined,
        body: {
          clientName: syncClientName,
          callbackUrl: syncCallbackUrl,
          currentVersion: Number(syncVersion),
          mode: syncMode,
        },
      });
      setMessage(
        `Sync finished. status=${response.downstreamStatus}, pushed=${String(response.pushed)}, currentVersion=${response.currentVersion}.`,
      );
      await loadSnapshot();
    });
  }

  async function handleRebuild() {
    await runAction('Rebuild', async () => {
      const response = await apiRequest<{
        processed: number;
        updated: number;
      }>('/api/admin/rebuild-secure', {
        method: 'POST',
        token: adminToken || undefined,
      });
      setMessage(
        `Rebuild completed. processed=${response.processed}, updated=${response.updated}.`,
      );
      await loadSnapshot();
    });
  }

  async function handleExport() {
    await runAction('Export', async () => {
      const response = await apiRequest<{
        objectKey: string;
        emailStatus: string;
      }>('/api/admin/export-now', {
        method: 'POST',
        token: adminToken || undefined,
      });
      setMessage(
        `Export archived to ${response.objectKey}. Email=${response.emailStatus}.`,
      );
    });
  }

  const overview = snapshot?.overview;
  const rawRows =
    snapshot?.rawRecords.map((record) => [
      record.recordKey,
      record.source,
      truncate(record.receivedAt, 30),
      truncate(toPrettyJson(record.payload), 120),
      truncate(toPrettyJson(record.payloadColumns), 120),
    ]) ?? [];

  const secureRows =
    snapshot?.secureRecords.map((record) => [
      record.recordKey,
      String(record.version),
      record.encryptFields.join(', ') || 'none',
      truncate(toPrettyJson(record.publicData), 100),
      truncate(toPrettyJson(record.publicColumns), 100),
      truncate(toPrettyJson(record.encryptedColumns), 100),
    ]) ?? [];

  const downstreamRows =
    snapshot?.downstreamClients.map((client) => [
      client.clientName ?? 'anonymous',
      client.callbackUrl,
      String(client.clientVersion),
      String(client.lastSyncVersion),
      client.lastStatus,
      client.lastPushAt ?? '-',
    ]) ?? [];

  return (
    <div className="app-shell">
      <div className="bg-orb orb-a" />
      <div className="bg-orb orb-b" />
      <div className="bg-orb orb-c" />

      <header className="hero glass-panel">
        <div className="hero-copy">
          <span className="eyebrow">Cloudflare Workers + D1 + R2</span>
          <h1>NCT API SQL Console</h1>
          <p>
            单个 Worker 承载数据接收、加密处理、版本同步、R2 归档与
            React 管理台。页面内直接支持数据库查看、分析、同步调试和导出触发。
          </p>
        </div>
        <div className="hero-actions">
          <div className="token-grid">
            <label className="token-field">
              <span>Admin token</span>
              <input
                type="password"
                value={adminToken}
                onChange={(event) => setAdminToken(event.target.value)}
                placeholder="Used by snapshot/export/rebuild"
              />
            </label>
            <label className="token-field">
              <span>Ingest token</span>
              <input
                type="password"
                value={ingestToken}
                onChange={(event) => setIngestToken(event.target.value)}
                placeholder="Blank means fallback to admin token"
              />
            </label>
            <label className="token-field">
              <span>Sync token</span>
              <input
                type="password"
                value={syncToken}
                onChange={(event) => setSyncToken(event.target.value)}
                placeholder="Blank means fallback to admin token"
              />
            </label>
          </div>
          <div className="action-row">
            <button
              type="button"
              className="primary-button"
              onClick={() => void loadSnapshot()}
              disabled={loading || busyAction !== null}
            >
              {loading ? 'Refreshing...' : 'Refresh snapshot'}
            </button>
            <button
              type="button"
              className="secondary-button"
              onClick={() => void handleRebuild()}
              disabled={busyAction !== null}
            >
              Rebuild secure table
            </button>
            <button
              type="button"
              className="secondary-button"
              onClick={() => void handleExport()}
              disabled={busyAction !== null}
            >
              Export + email
            </button>
          </div>
          <div className="status-strip">
            <span className={error ? 'status-badge danger' : 'status-badge'}>
              {busyAction ? `${busyAction} running` : 'Idle'}
            </span>
            <p>{error ?? message}</p>
          </div>
        </div>
      </header>

      <main className="content-grid">
        <section className="metric-grid">
          <MetricCard
            label="Raw records"
            value={overview?.totals.rawRecords ?? 0}
            helper="未加密原始表"
          />
          <MetricCard
            label="Secure records"
            value={overview?.totals.secureRecords ?? 0}
            helper="部分列加密后用于下游同步"
          />
          <MetricCard
            label="Downstream clients"
            value={overview?.totals.downstreamClients ?? 0}
            helper="记录下游回传的 URL 与同步状态"
          />
          <MetricCard
            label="Current version"
            value={overview?.totals.currentVersion ?? 0}
            helper="以 secure table 的最大版本号为准"
          />
        </section>

        <Suspense
          fallback={
            <section className="glass-panel analytics-panel loading-panel">
              <SectionTitle
                eyebrow="Analysis"
                title="数据分析与可视化"
                description="图表模块正在加载。"
              />
            </section>
          }
        >
          <AnalyticsSection overview={overview} />
        </Suspense>

        <section className="glass-panel form-panel">
          <SectionTitle
            eyebrow="Debug"
            title="数据写入与同步调试"
            description="直接从前端构造下游请求，验证写入、加密、版本推进和回推逻辑。"
          />
          <div className="form-grid">
            <form className="action-form" onSubmit={handleIngestSubmit}>
              <h3>POST /api/ingest</h3>
              <textarea
                value={ingestPayload}
                onChange={(event) => setIngestPayload(event.target.value)}
                spellCheck={false}
              />
              <button
                type="submit"
                className="primary-button"
                disabled={busyAction !== null}
              >
                Send ingest
              </button>
            </form>

            <form className="action-form" onSubmit={handleSyncSubmit}>
              <h3>POST /api/sync</h3>
              <label>
                <span>Client name</span>
                <input
                  value={syncClientName}
                  onChange={(event) => setSyncClientName(event.target.value)}
                />
              </label>
              <label>
                <span>Callback URL</span>
                <input
                  value={syncCallbackUrl}
                  onChange={(event) => setSyncCallbackUrl(event.target.value)}
                />
              </label>
              <label>
                <span>Client version</span>
                <input
                  type="number"
                  min={0}
                  value={syncVersion}
                  onChange={(event) => setSyncVersion(Number(event.target.value))}
                />
              </label>
              <label>
                <span>Mode</span>
                <select
                  value={syncMode}
                  onChange={(event) =>
                    setSyncMode(event.target.value as 'full' | 'delta')
                  }
                >
                  <option value="full">full snapshot</option>
                  <option value="delta">delta only</option>
                </select>
              </label>
              <button
                type="submit"
                className="primary-button"
                disabled={busyAction !== null}
              >
                Trigger sync
              </button>
            </form>
          </div>
        </section>

        <section className="table-grid">
          <TableBlock
            title="Raw table"
            columns={['recordKey', 'source', 'receivedAt', 'payload', 'payloadColumns']}
            rows={rawRows}
          />
          <TableBlock
            title="Secure table"
            columns={['recordKey', 'version', 'encryptFields', 'publicData', 'publicColumns', 'encryptedColumns']}
            rows={secureRows}
          />
          <TableBlock
            title="Downstream table"
            columns={['clientName', 'callbackUrl', 'clientVersion', 'lastSyncVersion', 'status', 'lastPushAt']}
            rows={downstreamRows}
          />
        </section>
      </main>
    </div>
  );
}
