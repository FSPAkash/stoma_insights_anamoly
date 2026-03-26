import React, { useState, useEffect, useMemo } from 'react';
import {
  ResponsiveContainer,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar,
  Tooltip,
  Legend,
} from 'recharts';
import GlassCard from './GlassCard';
import { getBetaRadarFingerprints } from '../utils/api';
import { formatSensorName, systemColor } from '../utils/formatters';

const styles = {
  tabRow: { display: 'flex', gap: '6px', flexWrap: 'wrap', marginBottom: '16px' },
  tab: (active, color) => ({
    padding: '6px 16px', borderRadius: '20px', fontSize: '12px',
    fontWeight: active ? 600 : 500, cursor: 'pointer',
    border: active ? `2px solid ${color}` : '1.5px solid rgba(203,230,200,0.6)',
    background: active ? `${color}14` : 'rgba(255,255,255,0.6)',
    color: active ? color : '#6B736B', transition: 'all 0.2s ease',
    backdropFilter: 'blur(8px)', userSelect: 'none',
  }),
  cardHeader: {
    display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '4px',
  },
  sysLabel: { fontSize: '14px', fontWeight: 700, letterSpacing: '0.02em' },
  meta: { fontSize: '10.5px', color: '#8A928A', marginBottom: '4px' },
  sensorTable: { width: '100%', borderCollapse: 'collapse', fontSize: '11px' },
  thCell: { padding: '6px 8px', textAlign: 'left', fontWeight: 600, color: '#8A928A', fontSize: '9.5px', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: '1px solid rgba(203,230,200,0.3)' },
  tdCell: { padding: '5px 8px', borderBottom: '1px solid rgba(203,230,200,0.15)', color: '#2c3e50' },
  trustBadge: (trust) => ({
    fontSize: '9px', fontWeight: 600, padding: '2px 6px', borderRadius: '8px',
    display: 'inline-block',
    backgroundColor: trust === 'Reliable' ? 'rgba(27,94,32,0.08)' : 'rgba(231,76,60,0.08)',
    color: trust === 'Reliable' ? '#1B5E20' : '#c0392b',
  }),
  loading: { display: 'flex', alignItems: 'center', justifyContent: 'center', height: '200px', color: '#8A928A', fontSize: '13px' },
  empty: { textAlign: 'center', padding: '40px 20px', color: '#8A928A', fontSize: '13px' },
  sectionTitle: { fontSize: '10px', fontWeight: 700, color: '#8A928A', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: '6px', marginTop: '2px' },
  topContributor: (color) => ({
    display: 'flex', alignItems: 'center', gap: '10px',
    padding: '8px 12px', borderRadius: '10px', marginBottom: '4px',
    background: `${color}08`, border: `1px solid ${color}20`,
  }),
  topContribLabel: { fontSize: '9.5px', fontWeight: 600, color: '#8A928A', textTransform: 'uppercase', letterSpacing: '0.06em' },
  topContribValue: { fontSize: '13px', fontWeight: 700, color: '#2c3e50' },
  topContribSub: { fontSize: '10px', color: '#8A928A' },
  collapseToggle: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    cursor: 'pointer', userSelect: 'none', padding: '6px 0',
  },
  chevron: (open) => ({
    fontSize: '10px', color: '#8A928A', transition: 'transform 0.2s ease',
    transform: open ? 'rotate(180deg)' : 'rotate(0deg)',
  }),
};

const CustomTooltip = ({ active, payload }) => {
  if (!active || !payload || !payload.length) return null;
  const item = payload[0]?.payload;
  if (!item) return null;
  return (
    <div style={{
      background: 'rgba(255,255,255,0.92)', backdropFilter: 'blur(12px)',
      border: '1px solid rgba(203,230,200,0.5)', borderRadius: '10px',
      padding: '8px 12px', fontSize: '11px', boxShadow: '0 4px 12px rgba(0,0,0,0.08)',
    }}>
      <div style={{ fontWeight: 600, color: '#2c3e50', marginBottom: '2px' }}>
        {formatSensorName(item.sensor)}
      </div>
      <div style={{ color: '#6B736B' }}>Fault fingerprint: {item.fault.toFixed(4)}</div>
      <div style={{ color: '#1B5E20' }}>Normal baseline: {item.baseline.toFixed(4)}</div>
      {item.sqs != null && <div style={{ color: '#6B736B' }}>SQS: {item.sqs.toFixed(4)}</div>}
      {item.trust && <div style={{ color: '#6B736B' }}>Trust: {item.trust}</div>}
    </div>
  );
};

function RadarFingerprints() {
  const [fingerprints, setFingerprints] = useState([]);
  const [loading, setLoading] = useState(true);
  const [activeIdx, setActiveIdx] = useState(0);
  const [detailOpen, setDetailOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;
    getBetaRadarFingerprints()
      .then((res) => {
        if (!cancelled) setFingerprints(res.data.fingerprints || []);
      })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, []);

  const fp = fingerprints[activeIdx] || null;

  const baselineVal = 0.5;

  const radarData = useMemo(() => {
    if (!fp) return [];
    return fp.sensors.map((s) => ({
      sensor: s.sensor,
      sensorLabel: formatSensorName(s.sensor),
      fault: s.value,
      baseline: baselineVal,
      sqs: fp.sensor_meta?.[s.sensor]?.sqs ?? null,
      trust: fp.sensor_meta?.[s.sensor]?.trust ?? null,
    }));
  }, [fp, baselineVal]);

  const sortedSensors = useMemo(() => {
    return radarData.slice().sort((a, b) => b.fault - a.fault);
  }, [radarData]);

  const topSensor = sortedSensors[0] || null;

  const maxVal = useMemo(() => {
    if (!radarData.length) return 1;
    return Math.max(...radarData.map((d) => d.fault), baselineVal, 0.5);
  }, [radarData, baselineVal]);

  if (loading) return <div style={styles.loading}>Loading radar fingerprints...</div>;
  if (!fingerprints.length) return <div style={styles.empty}>No subsystem fingerprint data available.</div>;

  const color = fp ? systemColor(fp.system_id, activeIdx) : '#1B5E20';

  // Build subtitle
  let subtitle = '';
  if (fp) {
    if (fp.has_alarm && fp.event_start) {
      subtitle = `Peak risk=${fp.peak_risk.toFixed(3)}  |  Event duration=${fp.event_start.substring(0, 16)} to ${fp.event_end.substring(0, 16)}`;
    } else {
      subtitle = 'No alarms detected -- showing latest AE contributions';
    }
  }

  return (
    <div>
      <div style={styles.tabRow}>
        {fingerprints.map((f, idx) => (
          <div
            key={f.system_id}
            style={styles.tab(idx === activeIdx, systemColor(f.system_id, idx))}
            onClick={() => { setActiveIdx(idx); setDetailOpen(false); }}
          >
            {f.system_id}
            {f.has_alarm && f.alarm_count > 0 && (
              <span style={{ marginLeft: '6px', fontSize: '10px', opacity: 0.7 }}>
                ({f.alarm_count})
              </span>
            )}
          </div>
        ))}
      </div>

      {fp && (
        <GlassCard padding="16px 16px 8px">
          <div style={styles.cardHeader}>
            <span style={{ ...styles.sysLabel, color }}>{fp.system_id}</span>
          </div>
          <div style={styles.meta}>{subtitle}</div>

          <ResponsiveContainer width="100%" height={400}>
            <RadarChart data={radarData} cx="50%" cy="50%" outerRadius="70%">
              <PolarGrid stroke="rgba(27,94,32,0.12)" />
              <PolarAngleAxis dataKey="sensorLabel" tick={{ fontSize: 9, fill: '#6B736B' }} />
              <PolarRadiusAxis
                angle={90}
                domain={[0, Math.ceil(maxVal * 10) / 10]}
                tick={{ fontSize: 8, fill: '#8A928A' }}
                tickCount={5}
              />
              <Radar
                name="Normal baseline (0.5s)"
                dataKey="baseline"
                stroke="#1B5E20"
                fill="#1B5E20"
                fillOpacity={0.06}
                strokeWidth={1.5}
                strokeDasharray="5 3"
                dot={false}
              />
              <Radar
                name="Fault fingerprint"
                dataKey="fault"
                stroke="#e74c3c"
                fill="#e74c3c"
                fillOpacity={0.15}
                strokeWidth={2}
                dot={{ r: 3, fill: '#e74c3c', strokeWidth: 0 }}
              />
              <Legend
                wrapperStyle={{ fontSize: '11px', paddingTop: '8px' }}
                iconType="line"
              />
              <Tooltip content={<CustomTooltip />} />
            </RadarChart>
          </ResponsiveContainer>

          {/* Top contributor callout */}
          {topSensor && (
            <div style={styles.topContributor(color)}>
              <div>
                <div style={styles.topContribLabel}>Top Contributor</div>
                <div style={styles.topContribValue}>{formatSensorName(topSensor.sensor)}</div>
                <div style={styles.topContribSub}>
                  AE: {topSensor.fault.toFixed(4)}
                  {topSensor.sqs != null && ` | SQS: ${topSensor.sqs.toFixed(2)}`}
                  {topSensor.trust && ` | ${topSensor.trust}`}
                </div>
              </div>
            </div>
          )}

          {/* Collapsible sensor detail */}
          <div style={{ marginTop: '8px' }}>
            <div
              style={styles.collapseToggle}
              onClick={() => setDetailOpen((v) => !v)}
            >
              <div style={styles.sectionTitle}>Sensor Detail ({sortedSensors.length})</div>
              <span style={styles.chevron(detailOpen)}>&#9660;</span>
            </div>
            {detailOpen && (
              <table style={styles.sensorTable}>
                <thead>
                  <tr>
                    <th style={styles.thCell}>Sensor</th>
                    <th style={{ ...styles.thCell, textAlign: 'right' }}>AE Contrib</th>
                    <th style={{ ...styles.thCell, textAlign: 'right' }}>SQS</th>
                    <th style={{ ...styles.thCell, textAlign: 'center' }}>Trust</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedSensors.map((s) => (
                    <tr key={s.sensor}>
                      <td style={styles.tdCell}>{formatSensorName(s.sensor)}</td>
                      <td style={{ ...styles.tdCell, textAlign: 'right', fontWeight: 600 }}>{s.fault.toFixed(4)}</td>
                      <td style={{ ...styles.tdCell, textAlign: 'right' }}>{s.sqs != null ? s.sqs.toFixed(2) : '--'}</td>
                      <td style={{ ...styles.tdCell, textAlign: 'center' }}>
                        {s.trust ? <span style={styles.trustBadge(s.trust)}>{s.trust}</span> : '--'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </GlassCard>
      )}
    </div>
  );
}

export default RadarFingerprints;
