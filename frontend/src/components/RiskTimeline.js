import React, { useMemo, useState } from 'react';
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ReferenceLine,
} from 'recharts';
import GlassCard from './GlassCard';
import InfoTooltip from './InfoTooltip';

const LINES = [
  { key: 'risk_score', label: 'Risk Score', color: '#1B5E20', fill: 'url(#riskGrad)', width: 2 },
  { key: 'risk_mech', label: 'Mechanical', color: '#4CAF50', fill: 'url(#mechGrad)', width: 1.2 },
  { key: 'risk_elec', label: 'Electrical', color: '#81C784', fill: 'none', width: 1.2 },
  { key: 'risk_therm', label: 'Thermal', color: '#FFA726', fill: 'none', width: 1, dash: '4 3' },
];

const styles = {
  heading: {
    fontSize: '12px',
    fontWeight: 500,
    color: '#6B736B',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    marginBottom: '16px',
    display: 'flex',
    alignItems: 'center',
  },
  legend: {
    display: 'flex',
    gap: '6px',
    flexWrap: 'wrap',
    marginBottom: '12px',
  },
  legendItem: (active) => ({
    display: 'flex',
    alignItems: 'center',
    gap: '6px',
    fontSize: '11px',
    color: active ? '#4a524a' : '#b0b8b0',
    cursor: 'pointer',
    padding: '3px 10px',
    borderRadius: '20px',
    background: active ? 'rgba(203,230,200,0.35)' : 'rgba(0,0,0,0.03)',
    border: active ? '1px solid rgba(129,199,132,0.4)' : '1px solid transparent',
    transition: 'all 0.2s ease',
    userSelect: 'none',
  }),
  legendDot: (color, active) => ({
    width: '8px',
    height: '8px',
    borderRadius: '50%',
    background: active ? color : '#ccc',
    transition: 'background 0.2s ease',
  }),
  refLineLabel: {
    fontSize: '10px',
    fontWeight: 500,
  },
  emptyState: {
    textAlign: 'center',
    padding: '48px 20px',
    color: '#8A928A',
    fontSize: '14px',
  },
};

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload || !payload.length) return null;
  return (
    <div
      style={{
        background: 'rgba(255,255,255,0.94)',
        backdropFilter: 'blur(20px)',
        WebkitBackdropFilter: 'blur(20px)',
        border: '1px solid rgba(203,230,200,0.5)',
        borderRadius: '14px',
        padding: '14px 18px',
        boxShadow: '0 12px 36px rgba(27,94,32,0.12)',
        fontSize: '12px',
      }}
    >
      <div style={{ fontWeight: 600, color: '#1B5E20', marginBottom: '8px' }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ display: 'flex', justifyContent: 'space-between', gap: '20px', marginBottom: '3px' }}>
          <span style={{ color: p.color, display: 'flex', alignItems: 'center', gap: '4px' }}>
            <span style={{ width: '6px', height: '6px', borderRadius: '50%', background: p.color, display: 'inline-block' }} />
            {p.name}
          </span>
          <span style={{ fontWeight: 500, fontVariantNumeric: 'tabular-nums' }}>
            {p.value !== null && p.value !== undefined ? Number(p.value).toFixed(4) : '--'}
          </span>
        </div>
      ))}
    </div>
  );
};

function RiskTimeline({ timeseries, timestampCol }) {
  const [visibleLines, setVisibleLines] = useState(() =>
    Object.fromEntries(LINES.map((l) => [l.key, true]))
  );
  const [selectedDay, setSelectedDay] = useState(null);

  const toggleLine = (key) => {
    setVisibleLines((prev) => {
      const activeCount = Object.values(prev).filter(Boolean).length;
      // Don't allow hiding the last visible line
      if (prev[key] && activeCount <= 1) return prev;
      return { ...prev, [key]: !prev[key] };
    });
  };

  // Extract unique days from timeseries
  const availableDays = useMemo(() => {
    if (!timeseries || !timeseries.length) return [];
    const daySet = new Set();
    timeseries.forEach((row) => {
      const ts = row[timestampCol];
      if (ts) daySet.add(String(ts).substring(0, 10));
    });
    return Array.from(daySet).sort();
  }, [timeseries, timestampCol]);

  // Default to most recent day
  React.useEffect(() => {
    if (availableDays.length && !availableDays.includes(selectedDay)) {
      setSelectedDay(availableDays[availableDays.length - 1]);
    }
  }, [availableDays, selectedDay]);

  const data = useMemo(() => {
    if (!timeseries || !timeseries.length || !selectedDay) return [];
    return timeseries
      .filter((row) => {
        const ts = row[timestampCol];
        return ts && String(ts).substring(0, 10) === selectedDay;
      })
      .map((row) => ({
        ...row,
        ts: row[timestampCol]
          ? String(row[timestampCol]).substring(11, 16)
          : '',
        fullTs: row[timestampCol] || '',
      }));
  }, [timeseries, timestampCol, selectedDay]);

  return (
    <GlassCard delay={0.45} style={{ marginTop: '8px' }} intensity="strong">
      <div style={styles.heading}>
        Risk Score Timeline
        <InfoTooltip text="Time series of the fused risk_score and per-subsystem risk components. risk_score = (0.35*mech + 0.35*elec + 0.15*therm + 0.15*instrument) * confidence * gate. The dashed red line marks the HIGH threshold (0.80) and orange marks MEDIUM (0.55). Risk is forced to 0 during downtime periods." />
      </div>

      {!data.length ? (
        <div style={styles.emptyState}>
          No timeseries data available. Ensure the scores.csv file is loaded in the backend data folder and contains a timestamp column.
        </div>
      ) : (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '8px', marginBottom: '12px' }}>
          <div style={{ ...styles.legend, marginBottom: 0 }}>
            {LINES.map((line) => (
              <div
                key={line.key}
                style={styles.legendItem(visibleLines[line.key])}
                onClick={() => toggleLine(line.key)}
              >
                <div style={styles.legendDot(line.color, visibleLines[line.key])} />
                <span>{line.label}</span>
              </div>
            ))}
            <div style={{ ...styles.legendItem(true), cursor: 'default', background: 'none', border: '1px solid transparent' }}>
              <div style={{ width: '16px', height: '2px', background: '#EF5350', borderTop: '1px dashed #EF5350' }} />
              <span>HIGH (0.80)</span>
            </div>
            <div style={{ ...styles.legendItem(true), cursor: 'default', background: 'none', border: '1px solid transparent' }}>
              <div style={{ width: '16px', height: '2px', background: '#FFA726', borderTop: '1px dashed #FFA726' }} />
              <span>MEDIUM (0.55)</span>
            </div>
          </div>
          {availableDays.length > 1 && (
            <select
              value={selectedDay}
              onChange={(e) => setSelectedDay(e.target.value)}
              style={{
                fontSize: '11px',
                padding: '5px 10px',
                borderRadius: '8px',
                border: '1px solid rgba(203,230,200,0.6)',
                background: 'rgba(255,255,255,0.7)',
                color: '#4a524a',
                cursor: 'pointer',
                outline: 'none',
                fontWeight: 500,
              }}
            >
              {availableDays.map((day) => {
                const d = new Date(day + 'T00:00:00');
                const label = d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
                return <option key={day} value={day}>{label}</option>;
              })}
            </select>
          )}
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={data} margin={{ top: 10, right: 20, bottom: 5, left: 0 }}>
              <defs>
                <linearGradient id="riskGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#1B5E20" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="#1B5E20" stopOpacity={0.02} />
                </linearGradient>
                <linearGradient id="mechGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#4CAF50" stopOpacity={0.2} />
                  <stop offset="100%" stopColor="#4CAF50" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(203,230,200,0.4)" />
              <XAxis
                dataKey="ts"
                tick={{ fontSize: 10, fill: '#8A928A' }}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fontSize: 10, fill: '#8A928A' }}
                tickLine={false}
                domain={[0, 1]}
                ticks={[0, 0.2, 0.4, 0.55, 0.8, 1.0]}
              />
              <Tooltip content={<CustomTooltip />} />
              <ReferenceLine y={0.8} stroke="#EF5350" strokeDasharray="6 3" strokeWidth={1.5} label={{ value: 'HIGH', position: 'right', style: { ...styles.refLineLabel, fill: '#EF5350' } }} />
              <ReferenceLine y={0.55} stroke="#FFA726" strokeDasharray="6 3" strokeWidth={1.5} label={{ value: 'MED', position: 'right', style: { ...styles.refLineLabel, fill: '#FFA726' } }} />
              {LINES.map((line) =>
                visibleLines[line.key] ? (
                  <Area
                    key={line.key}
                    type="monotone"
                    dataKey={line.key}
                    stroke={line.color}
                    fill={line.fill}
                    strokeWidth={line.width}
                    dot={false}
                    name={line.label}
                    strokeDasharray={line.dash}
                    animationDuration={800}
                  />
                ) : null
              )}
            </AreaChart>
          </ResponsiveContainer>
        </>
      )}
    </GlassCard>
  );
}

export default RiskTimeline;
