import React from 'react';
import { Terminal } from './Terminal';
import type { BucketInfo } from '../types';

interface BucketListProps {
  buckets: BucketInfo[];
  loading?: boolean;
}

export const BucketList: React.FC<BucketListProps> = ({ buckets, loading }) => {
  return (
    <Terminal title="buckets - bash" className="bucket-list">
      <div className="command-line">
        <span className="prompt">$</span>
        <span className="command">aws s3 ls</span>
        {loading && <span className="cursor blink">_</span>}
      </div>

      {buckets.length === 0 ? (
        <div className="output muted">No buckets registered</div>
      ) : (
        <div className="output bucket-output">
          {buckets.map((bucket) => (
            <div key={bucket.name} className="bucket-row">
              <span className="bucket-icon">s3://</span>
              <span className="bucket-name">{bucket.name}</span>
              <span className="bucket-info muted">
                {bucket.object_count.toLocaleString()} objects, {bucket.node_ids.length} nodes
              </span>
            </div>
          ))}
        </div>
      )}
    </Terminal>
  );
};
