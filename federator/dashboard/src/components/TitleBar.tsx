import React from 'react';

interface TitleBarProps {
  title: string;
}

export const TitleBar: React.FC<TitleBarProps> = ({ title }) => {
  return (
    <div className="terminal-titlebar">
      <div className="traffic-lights">
        <span className="light red"></span>
        <span className="light yellow"></span>
        <span className="light green"></span>
      </div>
      <div className="title">{title}</div>
      <div className="spacer"></div>
    </div>
  );
};
