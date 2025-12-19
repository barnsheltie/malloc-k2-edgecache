import React from 'react';
import { NodeList } from './components/NodeList';
import { StatsPanel } from './components/StatsPanel';
import { BucketList } from './components/BucketList';
import { EventLog } from './components/EventLog';
import { useClusterStats, useNodes, useBuckets, useEvents } from './hooks/useApi';

const App: React.FC = () => {
  const { stats, loading: statsLoading } = useClusterStats(5000);
  const { nodes, loading: nodesLoading } = useNodes(10000);
  const { buckets, loading: bucketsLoading } = useBuckets(10000);
  const { events, loading: eventsLoading } = useEvents(3000);

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1 className="dashboard-title">
          <span className="logo">&gt;_</span>
          EdgeCache Federator
        </h1>
        <div className="dashboard-status">
          <span className="dot"></span>
          <span>Live</span>
        </div>
      </header>

      <NodeList nodes={nodes} loading={nodesLoading} />

      <StatsPanel stats={stats} loading={statsLoading} />

      <BucketList buckets={buckets} loading={bucketsLoading} />

      <EventLog events={events} loading={eventsLoading} />
    </div>
  );
};

export default App;
