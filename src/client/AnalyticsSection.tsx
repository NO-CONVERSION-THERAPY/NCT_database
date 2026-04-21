import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { AnalyticsOverview } from '../shared/types';

export default function AnalyticsSection(props: {
  overview: AnalyticsOverview | undefined;
}) {
  const overview = props.overview;

  return (
    <section className="glass-panel analytics-panel">
      <div className="section-title">
        <span className="eyebrow">Analysis</span>
        <h2>数据分析与可视化</h2>
        <p>版本增长、来源分布和第三表状态会随着 D1 快照实时刷新。</p>
      </div>

      <div className="chart-grid">
        <div className="chart-card">
          <h3>Version history</h3>
          <ResponsiveContainer width="100%" height={240}>
            <AreaChart
              data={overview?.versionHistory ?? []}
              margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
            >
              <defs>
                <linearGradient id="versionFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#72f1cf" stopOpacity={0.72} />
                  <stop offset="95%" stopColor="#72f1cf" stopOpacity={0.05} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="rgba(255,255,255,0.08)" />
              <XAxis dataKey="recordKey" tick={{ fill: '#c6d7ea', fontSize: 11 }} />
              <YAxis tick={{ fill: '#c6d7ea', fontSize: 11 }} />
              <Tooltip />
              <Area
                type="monotone"
                dataKey="version"
                stroke="#72f1cf"
                fill="url(#versionFill)"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        <div className="chart-card">
          <h3>Raw source mix</h3>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={overview?.rawBySource ?? []}>
              <CartesianGrid stroke="rgba(255,255,255,0.08)" />
              <XAxis dataKey="source" tick={{ fill: '#c6d7ea', fontSize: 11 }} />
              <YAxis tick={{ fill: '#c6d7ea', fontSize: 11 }} />
              <Tooltip />
              <Bar dataKey="count" fill="#ffb86f" radius={[8, 8, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="chart-card">
          <h3>Third-table statuses</h3>
          <ResponsiveContainer width="100%" height={240}>
            <PieChart>
              <Pie
                data={overview?.syncStatuses ?? []}
                dataKey="count"
                nameKey="status"
                cx="50%"
                cy="50%"
                outerRadius={82}
                innerRadius={44}
              >
                {(overview?.syncStatuses ?? []).map((entry, index) => (
                  <Cell
                    key={entry.status}
                    fill={['#72f1cf', '#6db8ff', '#ffc56f', '#ff8b7a'][index % 4]}
                  />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>
    </section>
  );
}
