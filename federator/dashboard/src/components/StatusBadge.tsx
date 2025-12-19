import React from 'react';
import type { NodeStatus } from '../types';

interface StatusBadgeProps {
  status: NodeStatus;
}

export const StatusBadge: React.FC<StatusBadgeProps> = ({ status }) => {
  const getStatusInfo = () => {
    switch (status) {
      case 'healthy':
        return { symbol: '\u25cf', label: 'online', className: 'status-healthy' };
      case 'degraded':
        return { symbol: '\u25cb', label: 'stale', className: 'status-degraded' };
      case 'offline':
        return { symbol: '\u25cb', label: 'offline', className: 'status-offline' };
    }
  };

  const { symbol, label, className } = getStatusInfo();

  return (
    <span className={`status-badge ${className}`}>
      <span className="status-symbol">{symbol}</span>
      <span className="status-label">{label}</span>
    </span>
  );
};
