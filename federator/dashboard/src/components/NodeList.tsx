import React from 'react';
import { Terminal } from './Terminal';
import { StatusBadge } from './StatusBadge';
import type { NodeInfo } from '../types';

interface NodeListProps {
  nodes: NodeInfo[];
  loading?: boolean;
}

function formatTimeAgo(timestamp: number): string {
  const seconds = Math.floor((Date.now() - timestamp) / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

export const NodeList: React.FC<NodeListProps> = ({ nodes, loading }) => {
  return (
    <Terminal title="nodes - bash" className="node-list">
      <div className="command-line">
        <span className="prompt">$</span>
        <span className="command">kubectl get nodes</span>
        {loading && <span className="cursor blink">_</span>}
      </div>

      {nodes.length === 0 ? (
        <div className="output muted">No nodes connected</div>
      ) : (
        <div className="table">
          <div className="table-header">
            <span className="col-id">ID</span>
            <span className="col-type">TYPE</span>
            <span className="col-status">STATUS</span>
            <span className="col-time">LAST SEEN</span>
            <span className="col-stats">OBJECTS</span>
            <span className="col-size">SIZE</span>
            <span className="col-rate">HIT RATE</span>
          </div>
          {nodes.map((node) => (
            <div key={node.id} className="table-row">
              <span className="col-id node-id">{node.id}</span>
              <span className="col-type">
                <span className={`node-type type-${node.node_type}`}>
                  {node.node_type}
                </span>
              </span>
              <span className="col-status">
                <StatusBadge status={node.status} />
              </span>
              <span className="col-time muted">
                {formatTimeAgo(node.last_heartbeat)}
              </span>
              <span className="col-stats">
                {node.stats?.objects_count.toLocaleString() ?? '-'}
              </span>
              <span className="col-size">
                {node.stats ? formatBytes(node.stats.total_size_bytes) : '-'}
              </span>
              <span className="col-rate">
                {node.stats ? `${(node.stats.hit_rate * 100).toFixed(1)}%` : '-'}
              </span>
            </div>
          ))}
        </div>
      )}
    </Terminal>
  );
};
