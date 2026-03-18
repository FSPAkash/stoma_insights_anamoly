import React, { useState, useEffect, useMemo, useCallback } from 'react';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ReferenceArea,
} from 'recharts';
import GlassCard from './GlassCard';
import InfoTooltip from './InfoTooltip';
import { getBetaSubsystems, getBetaSubsystemBehavior } from '../utils/api';
import { formatSensorName, systemColor } from '../utils/formatters';

const SENSOR_PALETTE = [
  '#1B5E20', '#0D47A1', '#E65100', '#7B1FA2', '#004D40',
  '#BF360C', '#1A237E', '#33691E', '#880E4F', '#01579B',
  '#F57F17', '#4A148C', '#006064', '#3E2723',
];

const styles = {
  heading: {
    fontSize: '12px', fontWeight: 500, color: '#6B736B', textTransform: 'uppercase',
    letterSpacing: '0.05em', marginBottom: '16px', display: 'flex', alignItems: 'center',
  },
  tabRow: { display: 'flex', gap: '6px', flexWrap: 'wrap', marginBottom: '14px' },
  tab: (active, color) => ({
    padding: '6px 16px', borderRadius: '20px', fontSize: '12px',
    fontWeight: active ? 600 : 500, cursor: 'pointer',
    border: active ? `2px solid ${color}` : '1.5px solid rgba(203,230,200,0.6)',
    background: active ? `${color}14` : 'rgba(255,255,255,0.6)',
    color: active ? color : '#6B736B', transition: 'all 0.2s ease',
    backdropFilter: 'blur(8px)', userSelect: 'none',
  }),
  chartContainer: { position: 'relative', minHeight: '340px' },
  loadingOverlay: {
    display: 'flex', alignItems: 'center', justifyContent: 'center',
    height: '300px', color: '#8A928A', fontSize: '13px',
  },
  legendRow: {
    display: 'flex', gap: '6px', flexWrap: 'wrap', marginTop: '10px', justifyContent: 'center',
  },
  legendItem: {
    display: 'flex', alignItems: 'center', gap: '4px', fontSize: '10px', color: '#6B736B',
  },
  legendDot: (color) => ({ width: '8px', height: '8px', borderRadius: '50%', background: color }),
  emptyState: { textAlign: 'center', padding: '48px 20px', color: '#8A928A', fontSize: '14px' },
};

const filterBadgeStyle = {
  fontSize: '10px', fontWeight: 500, color: '#1B5E20',
  background: 'rgba(129,199,132,0.15)', border: '1px solid rgba(129,199,132,0.3)',
  borderRadius: '6px', padding: '3px 10px', marginLeft: 'auto',
  whiteSpace: 'nowrap', letterSpacing: '0.02em', textTransform: 'none',
};

const CustomTooltip = ({ active, payload, downtimeBands, alarmBands }) => {
  if (!active || !payload || !payload.length) return null;
  const row = payload[0]?.payload;
  const rawTs = row?.fullTs || row?.ts || '';
  const displayTs = rawTs ? String(rawTs).replace(/\+00:00$/, ' UTC').substring(0, 23) : '';
  const idx = row?._idx;
  const inDowntime = idx != null && downtimeBands.some((b) => idx >= b.start && idx <= b.end);
  const matchedAlarm = idx != null ? alarmBands.find((b) => idx >= b.start && idx <= b.end) : null;

  const zoneLabels = [];
  if (inDowntime) {
    zoneLabels.push({ text: 'DOWNTIME', color: '#616161', bg: 'rgba(158,158,158,0.15)', border: '1.5px solid rgba(120,120,120,0.4)' });
  }
  if (matchedAlarm) {
    zoneLabels.push({
      text: `System Alarm -- ${matchedAlarm.severity}`,
      color: '#D32F2F',
      bg: 'rgba(239,83,80,0.10)',
      border: '1px solid rgba(239,83,80,0.3)',
    });
  }

  return (
    <div style={{
      background: 'rgba(255,255,255,0.94)', backdropFilter: 'blur(20px)',
      border: '1px solid rgba(203,230,200,0.5)', borderRadius: '14px',
      padding: '12px 16px', boxShadow: '0 12px 36px rgba(27,94,32,0.12)',
      fontSize: '11px', maxWidth: '320px',
    }}>
      {zoneLabels.map((zl, zi) => (
        <div key={zi} style={{
          background: zl.bg, border: zl.border, borderRadius: '6px',
          padding: '4px 10px', marginBottom: '4px', fontSize: '10px',
          fontWeight: 700, color: zl.color, letterSpacing: '0.04em',
        }}>{zl.text}</div>
      ))}
      <div style={{ fontWeight: 600, color: '#1B5E20', marginBottom: '6px' }}>{displayTs}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ display: 'flex', justifyContent: 'space-between', gap: '16px', marginBottom: '2px' }}>
          <span style={{ color: p.color, display: 'flex', alignItems: 'center', gap: '4px' }}>
            <span style={{ width: '6px', height: '6px', borderRadius: '50%', background: p.color, display: 'inline-block' }} />
            {p.name}
          </span>
          <span style={{ fontWeight: 500, fontVariantNumeric: 'tabular-nums' }}>
            {p.value !== null && p.value !== undefined ? Number(p.value).toFixed(2) : '--'}
          </span>
        </div>
      ))}
    </div>
  );
};

function SubsystemBehaviorChartBeta({ filterLabel, onFilterClick, selectedDay, isLatestMode, lastNHours, startTime, endTime }) {
  const [subsystems, setSubsystems] = useState([]);
  const [selectedSystem, setSelectedSystem] = useState(null);
  const [sensorData, setSensorData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [visibleSensors, setVisibleSensors] = useState({});
  const [tickInterval, setTickInterval] = useState('auto');

  useEffect(() => {
    getBetaSubsystems().then(res => {
      const subs = (res.data.subsystems || []).filter(s => s.system_id !== 'ISOLATED');
      setSubsystems(subs);
      if (subs.length > 0 && !selectedSystem) setSelectedSystem(subs[0].system_id);
    }).catch(() => {});
  }, [selectedSystem]);

  const loadData = useCallback(async (systemId) => {
    if (!systemId) return;
    setLoading(true);
    try {
      const res = await getBetaSubsystemBehavior(systemId, 1);
      setSensorData(res.data);
      const vis = {};
      (res.data.sensors || []).forEach((s) => { vis[s] = true; });
      setVisibleSensors(vis);
    } catch {
      setSensorData(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (selectedSystem) loadData(selectedSystem);
  }, [selectedSystem, loadData]);

  const toggleSensor = (sensor) => {
    setVisibleSensors((prev) => {
      const activeCount = Object.values(prev).filter(Boolean).length;
      if (prev[sensor] && activeCount <= 1) return prev;
      return { ...prev, [sensor]: !prev[sensor] };
    });
  };

  const chartData = useMemo(() => {
    if (!sensorData?.timeseries?.length) return [];
    const tsCol = sensorData.timestamp_col;
    let rows = sensorData.timeseries;

    if (selectedDay) {
      rows = rows.filter((row) => {
        const ts = row[tsCol];
        return ts && String(ts).substring(0, 10) === selectedDay;
      });
      if (isLatestMode && lastNHours < 24 && rows.length > 0) {
        const lastTs = rows[rows.length - 1][tsCol];
        if (lastTs) {
          const end = new Date(String(lastTs));
          const start = new Date(end.getTime() - lastNHours * 60 * 60 * 1000);
          rows = rows.filter((row) => new Date(String(row[tsCol])) >= start);
        }
      } else if (!isLatestMode) {
        rows = rows.filter((row) => {
          const hhmm = String(row[tsCol]).substring(11, 16);
          return hhmm >= startTime && hhmm <= endTime;
        });
      }
    }

    return rows.map((row, i) => ({
      ...row, _idx: i,
      ts: row[tsCol] ? String(row[tsCol]).substring(11, 16) : '',
      fullTs: row[tsCol] || '',
    }));
  }, [sensorData, selectedDay, isLatestMode, lastNHours, startTime, endTime]);

  const findClosestIdx = useCallback((targetTs, data) => {
    if (!data.length) return null;
    const target = new Date(targetTs).getTime();
    let best = 0, bestDiff = Math.abs(new Date(data[0].fullTs).getTime() - target);
    for (let i = 1; i < data.length; i++) {
      const diff = Math.abs(new Date(data[i].fullTs).getTime() - target);
      if (diff < bestDiff) { best = i; bestDiff = diff; }
    }
    return data[best]._idx;
  }, []);

  const downtimeBands = useMemo(() => {
    if (!sensorData?.downtime_bands?.length || !chartData.length) return [];
    const dayFilter = selectedDay || '';
    return sensorData.downtime_bands
      .filter((b) => {
        if (!dayFilter) return true;
        return String(b.start).substring(0, 10) === dayFilter;
      })
      .map((b) => ({ start: findClosestIdx(b.start, chartData), end: findClosestIdx(b.end, chartData) }))
      .filter((b) => b.start !== null && b.end !== null);
  }, [sensorData, chartData, selectedDay, findClosestIdx]);

  const alarmBands = useMemo(() => {
    if (!sensorData?.alarm_bands?.length || !chartData.length) return [];
    const dayFilter = selectedDay || '';
    return sensorData.alarm_bands
      .filter((b) => {
        if (!dayFilter) return true;
        const bStart = String(b.start).substring(0, 10);
        const bEnd = String(b.end).substring(0, 10);
        return bStart === dayFilter || bEnd === dayFilter;
      })
      .map((b) => ({
        start: findClosestIdx(b.start, chartData),
        end: findClosestIdx(b.end, chartData),
        severity: b.severity,
      }))
      .filter((b) => b.start !== null && b.end !== null);
  }, [sensorData, chartData, selectedDay, findClosestIdx]);

  const sensors = sensorData?.sensors || [];

  const yDomain = useMemo(() => {
    if (!chartData.length || !sensors.length) return ['auto', 'auto'];
    const active = sensors.filter((s) => visibleSensors[s]);
    if (!active.length) return ['auto', 'auto'];
    let min = Infinity, max = -Infinity;
    for (const row of chartData) {
      for (const s of active) {
        const v = row[s];
        if (v != null && isFinite(v)) {
          if (v < min) min = v;
          if (v > max) max = v;
        }
      }
    }
    if (!isFinite(min) || !isFinite(max)) return ['auto', 'auto'];
    const range = max - min;
    const pad = range > 0 ? range * 0.05 : 1;
    return [Math.floor((min - pad) * 100) / 100, Math.ceil((max + pad) * 100) / 100];
  }, [chartData, sensors, visibleSensors]);

  const xTicks = useMemo(() => {
    if (tickInterval === 'auto' || !chartData.length) return undefined;
    const seen = new Set();
    const ticks = [];
    for (const row of chartData) {
      const ts = row.fullTs ? String(row.fullTs) : '';
      if (!ts) continue;
      const key = tickInterval === 'minute' ? ts.substring(11, 16) : ts.substring(11, 13);
      if (!seen.has(key)) { seen.add(key); ticks.push(row._idx); }
    }
    return ticks;
  }, [chartData, tickInterval]);

  return (
    <GlassCard delay={0.5} style={{ marginTop: '8px' }} intensity="strong">
      <div style={styles.heading}>
        Subsystem Behavior
        <InfoTooltip text="Live sensor traces for each subsystem. Gray bands indicate downtime. Red bands indicate system alarms (High) from the beta pipeline." />
        {filterLabel && <span style={{ ...filterBadgeStyle, cursor: 'pointer' }} onClick={onFilterClick}>{filterLabel}</span>}
      </div>

      <div style={styles.tabRow}>
        {subsystems.map((sys, idx) => {
          const color = systemColor(sys.system_id, idx);
          return (
            <div key={sys.system_id} style={styles.tab(selectedSystem === sys.system_id, color)}
              onClick={() => setSelectedSystem(sys.system_id)}>
              {sys.system_id.replace('_', ' ')} ({sys.sensor_count})
            </div>
          );
        })}
      </div>

      <div style={styles.chartContainer}>
        {loading ? (
          <div style={styles.loadingOverlay}>Loading sensor data...</div>
        ) : !chartData.length ? (
          <div style={styles.emptyState}>
            {selectedSystem ? `No sensor data for ${selectedSystem}.` : 'Select a subsystem above.'}
          </div>
        ) : (
          <>
            {/* Sensor toggle legend */}
            <div style={styles.legendRow}>
              {sensors.map((s, i) => {
                const color = SENSOR_PALETTE[i % SENSOR_PALETTE.length];
                const active = visibleSensors[s];
                return (
                  <div key={s} style={{
                    ...styles.legendItem, cursor: 'pointer',
                    opacity: active ? 1 : 0.4, padding: '2px 8px', borderRadius: '12px',
                    background: active ? `${color}12` : 'transparent',
                    border: active ? `1px solid ${color}30` : '1px solid transparent',
                    transition: 'all 0.2s',
                  }} onClick={() => toggleSensor(s)}>
                    <div style={styles.legendDot(active ? color : '#ccc')} />
                    <span>{formatSensorName(s)}</span>
                  </div>
                );
              })}
              <div style={{ ...styles.legendItem, cursor: 'default', marginLeft: '8px' }}>
                <div style={{ width: '14px', height: '8px', background: '#9E9E9E', borderRadius: '2px', border: '1px solid #757575', opacity: 0.7 }} />
                <span style={{ fontWeight: 600 }}>Downtime</span>
              </div>
              {alarmBands.length > 0 && (
                <div style={{ ...styles.legendItem, cursor: 'default' }}>
                  <div style={{ width: '14px', height: '8px', background: 'rgba(239,83,80,0.30)', borderRadius: '2px', border: '1px solid rgba(239,83,80,0.5)' }} />
                  <span style={{ fontWeight: 600 }}>System Alarm</span>
                </div>
              )}
            </div>

            <ResponsiveContainer width="100%" height={320}>
              <LineChart data={chartData} margin={{ top: 10, right: 20, bottom: 5, left: 0 }} style={{ cursor: 'crosshair' }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(203,230,200,0.4)" />
                <XAxis
                  dataKey="_idx" type="number" domain={['dataMin', 'dataMax']}
                  ticks={xTicks} tick={{ fontSize: 10, fill: '#8A928A' }} tickLine={false}
                  label={{ value: 'UTC', position: 'insideBottomRight', offset: -2, style: { fontSize: 9, fill: '#8A928A' } }}
                  tickFormatter={(idx) => {
                    const row = chartData[idx] || chartData.find((r) => r._idx === idx);
                    if (!row) return '';
                    if (tickInterval === 'hour') {
                      const h = String(row.fullTs || '').substring(11, 13);
                      return h ? h + ':00' : row.ts;
                    }
                    return row.ts;
                  }}
                />
                <YAxis domain={yDomain} tick={{ fontSize: 10, fill: '#8A928A' }} tickLine={false} />
                <Tooltip content={<CustomTooltip downtimeBands={downtimeBands} alarmBands={alarmBands} />} />

                {/* Downtime bands - prominent solid gray */}
                {downtimeBands.map((band, i) => (
                  <ReferenceArea
                    key={`dt-${i}`}
                    x1={band.start} x2={band.end}
                    fill="rgba(120,120,120,0.28)"
                    fillOpacity={1}
                    stroke="rgba(80,80,80,0.6)"
                    strokeWidth={1.5}
                    strokeDasharray="6 3"
                  />
                ))}

                {/* Alarm bands - High system alarms only (red) */}
                {alarmBands.map((band, i) => (
                  <ReferenceArea
                    key={`alarm-${i}`}
                    x1={band.start} x2={band.end}
                    fill="rgba(239,83,80,0.22)"
                    fillOpacity={1}
                    stroke="rgba(239,83,80,0.5)"
                    strokeWidth={1.5}
                  />
                ))}

                {/* Sensor lines */}
                {sensors.map((s, i) =>
                  visibleSensors[s] ? (
                    <Line
                      key={s}
                      type="monotone"
                      dataKey={s}
                      stroke={SENSOR_PALETTE[i % SENSOR_PALETTE.length]}
                      strokeWidth={1.5}
                      dot={false}
                      name={formatSensorName(s)}
                      animationDuration={600}
                      connectNulls={false}
                    />
                  ) : null
                )}
              </LineChart>
            </ResponsiveContainer>

            {/* X-axis interval toggle */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '4px', marginTop: '4px', justifyContent: 'center' }}>
              <span style={{ fontSize: '10px', color: '#8A928A', marginRight: '4px' }}>X-axis</span>
              {['auto', 'minute', 'hour'].map((mode) => (
                <div key={mode} onClick={() => setTickInterval(mode)} style={{
                  fontSize: '10px', padding: '2px 8px', borderRadius: '10px', cursor: 'pointer',
                  fontWeight: tickInterval === mode ? 600 : 400,
                  color: tickInterval === mode ? '#1B5E20' : '#8A928A',
                  background: tickInterval === mode ? 'rgba(27,94,32,0.08)' : 'transparent',
                  border: tickInterval === mode ? '1px solid rgba(27,94,32,0.2)' : '1px solid transparent',
                  transition: 'all 0.2s',
                }}>
                  {mode === 'auto' ? 'Auto' : mode === 'minute' ? '1 min' : '1 hr'}
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </GlassCard>
  );
}

export default SubsystemBehaviorChartBeta;
