import React from 'react';
import { Terminal } from './Terminal';
import type { FederatorEvent } from '../types';

interface EventLogProps {
  events: FederatorEvent[];
  loading?: boolean;
  maxEvents?: number;
}

function formatTime(timestamp: number): string {
  const date = new Date(timestamp);
  return date.toLocaleTimeString('en-US', {
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
}

function getEventIcon(type: FederatorEvent['type']): string {
  switch (type) {
    case 'node_connected':
      return '\u2192'; // right arrow
    case 'node_disconnected':
      return '\u2190'; // left arrow
    case 'node_registered':
      return '\u2713'; // checkmark
    case 'object_put':
      return '\u25b2'; // up triangle
    case 'object_delete':
      return '\u2715'; // x
    case 'invalidation':
      return '\u21bb'; // refresh
    case 'bucket_sync':
      return '\u21c4'; // bidirectional arrow
    default:
      return '\u2022'; // bullet
  }
}

function getEventClass(type: FederatorEvent['type']): string {
  switch (type) {
    case 'node_connected':
    case 'node_registered':
      return 'event-success';
    case 'node_disconnected':
      return 'event-warning';
    case 'object_delete':
      return 'event-error';
    case 'invalidation':
      return 'event-info';
    default:
      return '';
  }
}

export const EventLog: React.FC<EventLogProps> = ({ events, loading, maxEvents = 20 }) => {
  const displayEvents = events.slice(0, maxEvents);

  return (
    <Terminal title="events - bash" className="event-log">
      <div className="command-line">
        <span className="prompt">$</span>
        <span className="command">tail -f /var/log/federator.log</span>
        {loading && <span className="cursor blink">_</span>}
      </div>

      <div className="output event-output">
        {displayEvents.length === 0 ? (
          <div className="muted">Waiting for events...</div>
        ) : (
          displayEvents.map((event, index) => (
            <div key={`${event.timestamp}-${index}`} className={`event-row ${getEventClass(event.type)}`}>
              <span className="event-time">[{formatTime(event.timestamp)}]</span>
              <span className="event-icon">{getEventIcon(event.type)}</span>
              {event.node_id && (
                <span className="event-node">{event.node_id}</span>
              )}
              <span className="event-details">{event.details}</span>
            </div>
          ))
        )}
        <div className="log-cursor">
          <span className="cursor blink">_</span>
        </div>
      </div>
    </Terminal>
  );
};
