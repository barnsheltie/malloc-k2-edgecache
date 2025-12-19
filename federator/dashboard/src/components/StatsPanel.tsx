import React from 'react';
import { Terminal } from './Terminal';
import type { ClusterStats } from '../types';

interface StatsPanelProps {
  stats: ClusterStats | null;
  loading?: boolean;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function formatNumber(n: number): string {
  return n.toLocaleString();
}

export const StatsPanel: React.FC<StatsPanelProps> = ({ stats, loading }) => {
  return (
    <Terminal title="stats - bash" className="stats-panel">
      <div className="command-line">
        <span className="prompt">$</span>
        <span className="command">cluster stats</span>
        {loading && <span className="cursor blink">_</span>}
      </div>

      {stats ? (
        <div className="output stats-output">
          <div className="stat-section">
            <div className="section-title"># Cluster Overview</div>
            <div className="stat-row">
              <span className="stat-label">nodes:</span>
              <span className="stat-value highlight">{stats.nodes_total}</span>
              <span className="stat-detail">
                (<span className="healthy">{stats.nodes_healthy} healthy</span>
                {stats.nodes_degraded > 0 && <>, <span className="degraded">{stats.nodes_degraded} degraded</span></>}
                {stats.nodes_offline > 0 && <>, <span className="offline">{stats.nodes_offline} offline</span></>})
              </span>
            </div>
            <div className="stat-row">
              <span className="stat-label">storage_nodes:</span>
              <span className="stat-value">{stats.storage_nodes}</span>
            </div>
            <div className="stat-row">
              <span className="stat-label">cache_nodes:</span>
              <span className="stat-value">{stats.cache_nodes}</span>
            </div>
          </div>

          <div className="stat-section">
            <div className="section-title"># Data</div>
            <div className="stat-row">
              <span className="stat-label">buckets:</span>
              <span className="stat-value">{stats.buckets_total}</span>
            </div>
            <div className="stat-row">
              <span className="stat-label">objects:</span>
              <span className="stat-value highlight">{formatNumber(stats.objects_total)}</span>
            </div>
            <div className="stat-row">
              <span className="stat-label">total_size:</span>
              <span className="stat-value">{formatBytes(stats.total_size_bytes)}</span>
            </div>
          </div>

          <div className="stat-section">
            <div className="section-title"># Performance</div>
            <div className="stat-row">
              <span className="stat-label">avg_hit_rate:</span>
              <span className={`stat-value ${stats.avg_hit_rate >= 0.9 ? 'good' : stats.avg_hit_rate >= 0.7 ? 'warn' : 'bad'}`}>
                {(stats.avg_hit_rate * 100).toFixed(1)}%
              </span>
            </div>
            <div className="stat-row">
              <span className="stat-label">total_requests:</span>
              <span className="stat-value">{formatNumber(stats.total_requests)}</span>
            </div>
          </div>
        </div>
      ) : (
        <div className="output muted">Loading...</div>
      )}
    </Terminal>
  );
};
