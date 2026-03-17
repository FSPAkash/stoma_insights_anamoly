import React from 'react';
import { ResponsiveContainer, LineChart, Line, ReferenceArea, Tooltip } from 'recharts';

const SparkTooltip = ({ active, payload }) => {
  if (!active || !payload || !payload.length) return null;
  const row = payload[0]?.payload;
  const ts = row?.ts_full || row?.ts || '';
  return (
    <div style={{
      background: 'rgba(255,255,255,0.94)', backdropFilter: 'blur(12px)',
      border: '1px solid rgba(203,230,200,0.5)', borderRadius: '8px',
      padding: '5px 8px', fontSize: '9px', boxShadow: '0 4px 12px rgba(0,0,0,0.08)',
    }}>
      <div style={{ fontWeight: 600, color: '#1B5E20', marginBottom: '2px' }}>{ts}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ display: 'flex', justifyContent: 'space-between', gap: '8px' }}>
          <span style={{ color: p.color }}>{p.name}</span>
          <span style={{ fontWeight: 500, fontVariantNumeric: 'tabular-nums' }}>
            {p.value != null ? Number(p.value).toFixed(3) : '--'}
          </span>
        </div>
      ))}
    </div>
  );
};

function Sparkline({ data, dataKey, color = '#1976D2', downtimeBands = [], height = 28, width = 120 }) {
  return (
    <div style={{ width, height }}>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 2, right: 2, bottom: 2, left: 2 }}>
          <Tooltip content={<SparkTooltip />} />
          {downtimeBands.map((band, i) => (
            <ReferenceArea key={i} x1={band.start} x2={band.end} fill="rgba(239,83,80,0.12)" fillOpacity={1} strokeOpacity={0} />
          ))}
          <Line type="monotone" dataKey={dataKey} stroke={color} strokeWidth={1.2} dot={false} connectNulls={false} isAnimationActive={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export default Sparkline;
