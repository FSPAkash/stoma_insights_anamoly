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
import { getSystemSensorValues } from '../utils/api';
import { formatSensorName, systemColor } from '../utils/formatters';

const SENSOR_PALETTE = [
  '#1B5E20', '#0D47A1', '#E65100', '#7B1FA2', '#004D40',
  '#BF360C', '#1A237E', '#33691E', '#880E4F', '#01579B',
  '#F57F17', '#4A148C', '#006064', '#3E2723',
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
  tabRow: {
    display: 'flex',
    gap: '6px',
    flexWrap: 'wrap',
    marginBottom: '14px',
  },
  tab: (active, color) => ({
    padding: '6px 16px',
    borderRadius: '20px',
    fontSize: '12px',
    fontWeight: active ? 600 : 500,
    cursor: 'pointer',
    border: active ? `2px solid ${color}` : '1.5px solid rgba(203,230,200,0.6)',
    background: active ? `${color}14` : 'rgba(255,255,255,0.6)',
    color: active ? color : '#6B736B',
    transition: 'all 0.2s ease',
    backdropFilter: 'blur(8px)',
    WebkitBackdropFilter: 'blur(8px)',
    userSelect: 'none',
  }),
  chartContainer: {
    position: 'relative',
    minHeight: '340px',
  },
  loadingOverlay: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    height: '300px',
    color: '#8A928A',
    fontSize: '13px',
  },
  legendRow: {
    display: 'flex',
    gap: '6px',
    flexWrap: 'wrap',
    marginTop: '10px',
    justifyContent: 'center',
  },
  legendItem: {
    display: 'flex',
    alignItems: 'center',
    gap: '4px',
    fontSize: '10px',
    color: '#6B736B',
  },
  legendDot: (color) => ({
    width: '8px',
    height: '8px',
    borderRadius: '50%',
    background: color,
  }),
  emptyState: {
    textAlign: 'center',
    padding: '48px 20px',
    color: '#8A928A',
    fontSize: '14px',
  },
};

const filterBadgeStyle = {
  fontSize: '10px',
  fontWeight: 500,
  color: '#1B5E20',
  background: 'rgba(129,199,132,0.15)',
  border: '1px solid rgba(129,199,132,0.3)',
  borderRadius: '6px',
  padding: '3px 10px',
  marginLeft: 'auto',
  whiteSpace: 'nowrap',
  letterSpacing: '0.02em',
  textTransform: 'none',
};

const BaseCustomTooltip = ({ active, payload, alertBands, downtimeBands }) => {
  if (!active || !payload || !payload.length) return null;
  const row = payload[0]?.payload;
  const rawTs = row?.fullTs || row?.ts || '';
  const displayTs = rawTs ? String(rawTs).replace(/\+00:00$/, ' UTC').substring(0, 21) : '';
  const idx = row?._idx;

  // Determine zone context
  const inDowntime = idx != null && downtimeBands.some((b) => idx >= b.start && idx <= b.end);
  const matchedAlert = idx != null ? alertBands.find((b) => idx >= b.start && idx <= b.end) : null;

  let zoneBg = null;
  let zoneLabel = null;
  let zoneBorder = null;
  if (matchedAlert) {
    const isProcess = matchedAlert.cls === 'PROCESS';
    const isHigh = matchedAlert.severity === 'HIGH';
    if (isProcess) {
      zoneBg = 'rgba(156,39,176,0.08)';
      zoneBorder = '1px solid rgba(156,39,176,0.25)';
      zoneLabel = { text: 'Process Alert', color: '#9C27B0' };
    } else {
      zoneBg = isHigh ? 'rgba(239,83,80,0.10)' : 'rgba(255,167,38,0.10)';
      zoneBorder = isHigh ? '1px solid rgba(239,83,80,0.3)' : '1px solid rgba(255,167,38,0.3)';
      zoneLabel = { text: `System Alert \u00B7 ${matchedAlert.severity}`, color: isHigh ? '#D32F2F' : '#E65100' };
    }
  } else if (inDowntime) {
    zoneBg = 'rgba(0,0,0,0.04)';
    zoneBorder = '1px solid rgba(0,0,0,0.12)';
    zoneLabel = { text: 'Downtime', color: '#616161' };
  }

  return (
    <div
      style={{
        background: 'rgba(255,255,255,0.94)',
        backdropFilter: 'blur(20px)',
        WebkitBackdropFilter: 'blur(20px)',
        border: '1px solid rgba(203,230,200,0.5)',
        borderRadius: '14px',
        padding: '12px 16px',
        boxShadow: '0 12px 36px rgba(27,94,32,0.12)',
        fontSize: '11px',
        maxWidth: '320px',
      }}
    >
      {zoneLabel && (
        <div style={{
          background: zoneBg,
          border: zoneBorder,
          borderRadius: '6px',
          padding: '3px 8px',
          marginBottom: '8px',
          fontSize: '10px',
          fontWeight: 600,
          color: zoneLabel.color,
          letterSpacing: '0.03em',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: '8px',
        }}>
          {zoneLabel.text}
          {matchedAlert && (
            <span style={{
              fontSize: '9px',
              fontWeight: 500,
              opacity: 0.7,
              letterSpacing: '0.04em',
            }}>
              click to open
            </span>
          )}
        </div>
      )}
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

function SubsystemBehaviorChart({ systems, filterLabel, selectedDay, isLatestMode, lastNHours, startTime, endTime, onFilterClick, alerts, onSelectAlert }) {
  const [selectedSystem, setSelectedSystem] = useState(null);
  const [sensorData, setSensorData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [visibleSensors, setVisibleSensors] = useState({});
  const [tickInterval, setTickInterval] = useState('auto');

  // Auto-select first system
  useEffect(() => {
    if (systems && systems.length > 0 && !selectedSystem) {
      setSelectedSystem(systems[0].system_id);
    }
  }, [systems, selectedSystem]);

  // Load sensor data when system changes
  const loadSystemData = useCallback(async (systemId) => {
    if (!systemId) return;
    setLoading(true);
    try {
      const res = await getSystemSensorValues(systemId, 5);
      setSensorData(res.data);
      // Initialize all sensors as visible
      const vis = {};
      (res.data.sensors || []).forEach((s) => { vis[s] = true; });
      setVisibleSensors(vis);
    } catch (err) {
      console.error('Failed to load system sensor data:', err);
      setSensorData(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (selectedSystem) {
      loadSystemData(selectedSystem);
    }
  }, [selectedSystem, loadSystemData]);

  const toggleSensor = (sensor) => {
    setVisibleSensors((prev) => {
      const activeCount = Object.values(prev).filter(Boolean).length;
      if (prev[sensor] && activeCount <= 1) return prev;
      return { ...prev, [sensor]: !prev[sensor] };
    });
  };

  const chartData = useMemo(() => {
    if (!sensorData || !sensorData.timeseries || !sensorData.timeseries.length) return [];
    const tsCol = sensorData.timestamp_col;
    let rows = sensorData.timeseries;

    // Apply time filter: filter by selectedDay first
    if (selectedDay) {
      rows = rows.filter((row) => {
        const ts = row[tsCol];
        return ts && String(ts).substring(0, 10) === selectedDay;
      });

      // Then apply time range
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
      ...row,
      _idx: i,
      ts: row[tsCol] ? String(row[tsCol]).substring(11, 16) : '',
      fullTs: row[tsCol] || '',
    }));
  }, [sensorData, selectedDay, isLatestMode, lastNHours, startTime, endTime]);

  // Helper: find the chart data index whose fullTs is closest to a target timestamp
  const findClosestIdx = useCallback((targetTs, data) => {
    if (!data.length) return null;
    const target = new Date(targetTs).getTime();
    let best = 0;
    let bestDiff = Math.abs(new Date(data[0].fullTs).getTime() - target);
    for (let i = 1; i < data.length; i++) {
      const diff = Math.abs(new Date(data[i].fullTs).getTime() - target);
      if (diff < bestDiff) { best = i; bestDiff = diff; }
    }
    return data[best]._idx;
  }, []);

  // Filter and map downtime bands to _idx values that exist in chartData
  const downtimeBands = useMemo(() => {
    if (!sensorData || !sensorData.downtime_bands || !chartData.length) return [];
    const dayFilter = selectedDay || '';
    return sensorData.downtime_bands
      .filter((b) => {
        if (!dayFilter) return true;
        const bDay = String(b.start).substring(0, 10);
        return bDay === dayFilter;
      })
      .map((b) => ({
        start: findClosestIdx(b.start, chartData),
        end: findClosestIdx(b.end, chartData),
      }))
      .filter((b) => b.start !== null && b.end !== null);
  }, [sensorData, chartData, selectedDay, findClosestIdx]);

  const alertBands = useMemo(() => {
    if (!sensorData || !sensorData.alert_bands || !chartData.length) return [];
    const dayFilter = selectedDay || '';
    return sensorData.alert_bands
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
        cls: b.class,
        rawStart: b.start,
        rawEnd: b.end,
      }))
      .filter((b) => b.start !== null && b.end !== null);
  }, [sensorData, chartData, selectedDay, findClosestIdx]);

  const sensors = sensorData?.sensors || [];

  // Handle chart click: if inside an alert band, open the corresponding alert episode
  const handleChartClick = useCallback((e) => {
    if (!e || !e.activePayload || !e.activePayload.length || !onSelectAlert) return;
    const idx = e.activePayload[0]?.payload?._idx;
    if (idx == null) return;
    const band = alertBands.find((b) => idx >= b.start && idx <= b.end);
    if (!band || !alerts || !alerts.length) return;
    // Match to the full alert object by start_ts and end_ts
    const match = alerts.find((a) => {
      const aStart = String(a.start_ts || '').substring(0, 19);
      const bStart = String(band.rawStart || '').substring(0, 19);
      const aEnd = String(a.end_ts || '').substring(0, 19);
      const bEnd = String(band.rawEnd || '').substring(0, 19);
      return aStart === bStart && aEnd === bEnd;
    });
    if (match) onSelectAlert(match);
  }, [alertBands, alerts, onSelectAlert]);

  // Compute explicit tick positions based on interval
  const xTicks = useMemo(() => {
    if (tickInterval === 'auto' || !chartData.length) return undefined;
    const seen = new Set();
    const ticks = [];
    for (const row of chartData) {
      const ts = row.fullTs ? String(row.fullTs) : '';
      if (!ts) continue;
      let key;
      if (tickInterval === 'minute') {
        key = ts.substring(11, 16); // HH:MM
      } else {
        key = ts.substring(11, 13); // HH
      }
      if (!seen.has(key)) {
        seen.add(key);
        ticks.push(row._idx);
      }
    }
    return ticks;
  }, [chartData, tickInterval]);

  if (!systems || !systems.length) {
    return (
      <GlassCard delay={0.45} style={{ marginTop: '8px' }} intensity="strong">
        <div style={styles.heading}>
          Subsystem Behavior
          <InfoTooltip text="Raw sensor traces for each discovered system with downtime and alert overlays." />
        </div>
        <div style={styles.emptyState}>No system data available.</div>
      </GlassCard>
    );
  }

  return (
    <GlassCard delay={0.45} style={{ marginTop: '8px' }} intensity="strong">
      <div style={styles.heading}>
        Subsystem Behavior
        <InfoTooltip text="Raw sensor traces for each dynamically discovered system. Gray bands indicate machine downtime periods. Red/orange bands indicate alert episodes classified to this system or PROCESS-level alerts." />
        {filterLabel && <span style={{ ...filterBadgeStyle, cursor: 'pointer' }} onClick={onFilterClick}>{filterLabel}</span>}
      </div>

      {/* System tabs */}
      <div style={styles.tabRow}>
        {systems.map((sys, idx) => {
          const color = systemColor(sys.system_id, idx);
          return (
            <div
              key={sys.system_id}
              style={styles.tab(selectedSystem === sys.system_id, color)}
              onClick={() => setSelectedSystem(sys.system_id)}
            >
              {sys.system_id.replace('_', ' ')} ({sys.sensor_count})
            </div>
          );
        })}
      </div>

      <div style={styles.chartContainer}>
        {loading ? (
          <div style={styles.loadingOverlay}>Loading sensor data...</div>
        ) : !chartData.length ? (
          <div style={styles.emptyState}>No sensor data for {selectedSystem}.</div>
        ) : (
          <>
            {/* Sensor toggle legend */}
            <div style={styles.legendRow}>
              {sensors.map((s, i) => {
                const color = SENSOR_PALETTE[i % SENSOR_PALETTE.length];
                const active = visibleSensors[s];
                return (
                  <div
                    key={s}
                    style={{
                      ...styles.legendItem,
                      cursor: 'pointer',
                      opacity: active ? 1 : 0.4,
                      padding: '2px 8px',
                      borderRadius: '12px',
                      background: active ? `${color}12` : 'transparent',
                      border: active ? `1px solid ${color}30` : '1px solid transparent',
                      transition: 'all 0.2s',
                    }}
                    onClick={() => toggleSensor(s)}
                  >
                    <div style={styles.legendDot(active ? color : '#ccc')} />
                    <span>{formatSensorName(s)}</span>
                  </div>
                );
              })}
              <div style={{ ...styles.legendItem, cursor: 'default', marginLeft: '8px' }}>
                <div style={{ width: '14px', height: '8px', background: 'rgba(0,0,0,0.06)', borderRadius: '2px' }} />
                <span>Downtime</span>
              </div>
              <div style={{ ...styles.legendItem, cursor: 'default' }}>
                <div style={{ width: '14px', height: '8px', background: 'rgba(239,83,80,0.18)', borderRadius: '2px' }} />
                <span>System Alert</span>
              </div>
              <div style={{ ...styles.legendItem, cursor: 'default' }}>
                <div style={{ width: '14px', height: '8px', background: 'rgba(156,39,176,0.12)', borderRadius: '2px', border: '1px dashed rgba(156,39,176,0.3)' }} />
                <span>Process Alert</span>
              </div>
            </div>

            <ResponsiveContainer width="100%" height={320}>
              <LineChart data={chartData} margin={{ top: 10, right: 20, bottom: 5, left: 0 }} onClick={handleChartClick} style={{ cursor: 'crosshair' }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(203,230,200,0.4)" />
                <XAxis
                  dataKey="_idx"
                  type="number"
                  domain={['dataMin', 'dataMax']}
                  ticks={xTicks}
                  tick={{ fontSize: 10, fill: '#8A928A' }}
                  tickLine={false}
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
                <YAxis
                  tick={{ fontSize: 10, fill: '#8A928A' }}
                  tickLine={false}
                />
                <Tooltip content={<BaseCustomTooltip alertBands={alertBands} downtimeBands={downtimeBands} />} />

                {/* Downtime bands */}
                {downtimeBands.map((band, i) => (
                  <ReferenceArea
                    key={`dt-${i}`}
                    x1={band.start}
                    x2={band.end}
                    fill="rgba(0,0,0,0.06)"
                    fillOpacity={1}
                    strokeOpacity={0}
                  />
                ))}

                {/* Alert bands - system-specific (solid) */}
                {alertBands.filter((b) => b.cls !== 'PROCESS').map((band, i) => (
                  <ReferenceArea
                    key={`alert-sys-${i}`}
                    x1={band.start}
                    x2={band.end}
                    fill={band.severity === 'HIGH' ? 'rgba(239,83,80,0.18)' : 'rgba(255,167,38,0.15)'}
                    fillOpacity={1}
                    strokeOpacity={0}
                  />
                ))}
                {/* Alert bands - PROCESS (lighter) */}
                {alertBands.filter((b) => b.cls === 'PROCESS').map((band, i) => (
                  <ReferenceArea
                    key={`alert-proc-${i}`}
                    x1={band.start}
                    x2={band.end}
                    fill="rgba(156,39,176,0.08)"
                    fillOpacity={1}
                    stroke="rgba(156,39,176,0.2)"
                    strokeDasharray="4 3"
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
                <div
                  key={mode}
                  onClick={() => setTickInterval(mode)}
                  style={{
                    fontSize: '10px',
                    padding: '2px 8px',
                    borderRadius: '10px',
                    cursor: 'pointer',
                    fontWeight: tickInterval === mode ? 600 : 400,
                    color: tickInterval === mode ? '#1B5E20' : '#8A928A',
                    background: tickInterval === mode ? 'rgba(27,94,32,0.08)' : 'transparent',
                    border: tickInterval === mode ? '1px solid rgba(27,94,32,0.2)' : '1px solid transparent',
                    transition: 'all 0.2s',
                  }}
                >
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

export default SubsystemBehaviorChart;
