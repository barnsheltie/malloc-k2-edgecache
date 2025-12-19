import React from 'react';
import { TitleBar } from './TitleBar';

interface TerminalProps {
  title: string;
  children: React.ReactNode;
  className?: string;
}

export const Terminal: React.FC<TerminalProps> = ({ title, children, className = '' }) => {
  return (
    <div className={`terminal ${className}`}>
      <TitleBar title={title} />
      <div className="terminal-content">
        {children}
      </div>
    </div>
  );
};
