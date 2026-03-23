import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
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
import { getBetaSubsystemBehavior, getBetaAlerts } from '../utils/api';
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

const controlPillStyle = (active, accent = '#1B5E20') => ({
  display: 'inline-flex',
  alignItems: 'center',
  gap: '6px',
  fontSize: '10px',
  padding: '4px 12px',
  borderRadius: '20px',
  cursor: 'pointer',
  border: active ? `1.5px solid ${accent}` : '1px solid rgba(203,230,200,0.6)',
  background: active ? 'rgba(27,94,32,0.08)' : 'rgba(255,255,255,0.6)',
  color: active ? accent : '#8A928A',
  fontWeight: 600,
  transition: 'all 0.2s',
});

const getAlarmStyle = (severity) => (
  severity === 'HIGH'
    ? {
        text: '#D32F2F',
        bg: 'rgba(239,83,80,0.10)',
        border: '1px solid rgba(239,83,80,0.3)',
        swatchBg: 'rgba(239,83,80,0.30)',
        swatchBorder: '1px solid rgba(239,83,80,0.5)',
        areaFill: 'rgba(239,83,80,0.22)',
        areaStroke: 'rgba(239,83,80,0.5)',
      }
    : severity === 'MEDIUM'
      ? {
        text: '#E65100',
        bg: 'rgba(255,167,38,0.14)',
        border: '1px solid rgba(255,167,38,0.32)',
        swatchBg: 'rgba(255,167,38,0.34)',
        swatchBorder: '1px solid rgba(245,124,0,0.45)',
        areaFill: 'rgba(255,167,38,0.20)',
        areaStroke: 'rgba(245,124,0,0.46)',
      }
      : severity === 'MIXED'
        ? {
        text: '#455A64',
        bg: 'rgba(120,144,156,0.16)',
        border: '1px solid rgba(120,144,156,0.32)',
        swatchBg: 'rgba(120,144,156,0.30)',
        swatchBorder: '1px solid rgba(84,110,122,0.45)',
        areaFill: 'rgba(120,144,156,0.20)',
        areaStroke: 'rgba(84,110,122,0.46)',
      }
      : {
        text: '#9A6A00',
        bg: 'rgba(255,213,79,0.18)',
        border: '1px solid rgba(255,193,7,0.34)',
        swatchBg: 'rgba(255,213,79,0.36)',
        swatchBorder: '1px solid rgba(255,179,0,0.45)',
        areaFill: 'rgba(255,213,79,0.22)',
        areaStroke: 'rgba(255,179,0,0.46)',
      }
);

const buildAlarmSummary = (alarm) => {
  if (!alarm) return '';
  const minuteCount = Number(alarm.minute_count || 1);
  if (minuteCount <= 1) return '';
  const mix = alarm.severity_mix ? String(alarm.severity_mix) : '';
  return mix ? `${minuteCount} alerts - ${mix}` : `${minuteCount} alerts`;
};

const CustomTooltip = ({ active, payload, downtimeBands, alarmBands, alarmView }) => {
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
    const alarmStyle = getAlarmStyle(matchedAlarm.severity);
    zoneLabels.push({
      text: alarmView === 'span'
        ? `Alarm Span -- ${matchedAlarm.severity}`
        : `System Alarm -- ${matchedAlarm.severity}`,
      color: alarmStyle.text,
      bg: alarmStyle.bg,
      border: alarmStyle.border,
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
      {matchedAlarm && buildAlarmSummary(matchedAlarm) && (
        <div style={{ fontSize: '10px', color: '#6B736B', marginBottom: '4px', fontWeight: 600 }}>
          {buildAlarmSummary(matchedAlarm)}
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

function SubsystemBehaviorChartBeta({ filterLabel, onFilterClick, selectedDay, isLatestMode, lastNHours, startTime, endTime, onZoomChange, onZoomReset, isZoomed, onSelectAlert, subsystems: subsystemsProp }) {
  const [alarmView, setAlarmView] = useState('minute');
  const subsystems = useMemo(() => (subsystemsProp || []).filter(s => s.system_id !== 'ISOLATED'), [subsystemsProp]);
  const [selectedSystem, setSelectedSystem] = useState(null);
  const [sensorData, setSensorData] = useState(null);
  const [alerts, setAlerts] = useState([]);
  const [loading, setLoading] = useState(false);
  const [visibleSensors, setVisibleSensors] = useState({});
  const [refAreaLeft, setRefAreaLeft] = useState(null);
  const [refAreaRight, setRefAreaRight] = useState(null);
  const behaviorCacheRef = useRef({});
  const isDraggingRef = useRef(false);
  const dragStartRef = useRef(null);

  useEffect(() => {
    if (subsystems.length > 0 && !selectedSystem) setSelectedSystem(subsystems[0].system_id);
  }, [subsystems, selectedSystem]);

  const loadData = useCallback(async (systemId) => {
    if (!systemId) return;
    // Use cached data if available for instant tab switching
    if (behaviorCacheRef.current[systemId]) {
      const cached = behaviorCacheRef.current[systemId];
      setSensorData(cached);
      const vis = {};
      (cached.sensors || []).forEach((s) => { vis[s] = true; });
      setVisibleSensors(vis);
      return;
    }
    setLoading(true);
    try {
      const res = await getBetaSubsystemBehavior(systemId, 1);
      behaviorCacheRef.current[systemId] = res.data;
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

  useEffect(() => {
    let cancelled = false;
    getBetaAlerts({ alarm_view: alarmView })
      .then((res) => {
        if (!cancelled) setAlerts(res.data.alerts || []);
      })
      .catch(() => {
        if (!cancelled) setAlerts([]);
      });
    return () => { cancelled = true; };
  }, [alarmView]);

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
    if (!alerts?.length || !chartData.length || !selectedSystem) return [];
    const sysAlerts = alerts.filter((a) => a.class === selectedSystem);
    if (!sysAlerts.length) return [];
    const dayFilter = selectedDay || '';
    return sysAlerts
      .filter((a) => {
        if (!dayFilter) return true;
        const aStart = String(a.start_ts || '').substring(0, 10);
        const aEnd = String(a.end_ts || '').substring(0, 10);
        return aStart === dayFilter || aEnd === dayFilter;
      })
      .map((a) => ({
        start: findClosestIdx(a.start_ts, chartData),
        end: findClosestIdx(a.end_ts, chartData),
        severity: a.severity || 'MEDIUM',
        minute_count: a.minute_count || 1,
        severity_mix: a.severity_mix || '',
        high_count: a.high_count || 0,
        medium_count: a.medium_count || 0,
        low_count: a.low_count || 0,
        view_type: a.view_type || alarmView,
        rawStart: a.start_ts,
        rawEnd: a.end_ts,
      }))
      .filter((b) => b.start !== null && b.end !== null)
      .map((b) => {
        if (b.start === b.end) {
          return { ...b, start: b.start - 0.45, end: b.end + 0.45 };
        }
        return b;
      });
  }, [alerts, chartData, selectedDay, selectedSystem, alarmView, findClosestIdx]);

  const sensors = sensorData?.sensors || [];
  const hasHighAlarms = alarmBands.some((band) => band.severity === 'HIGH');
  const hasMediumAlarms = alarmBands.some((band) => band.severity === 'MEDIUM');
  const hasLowAlarms = alarmBands.some((band) => band.severity === 'LOW');
  const hasMixedAlarms = alarmBands.some((band) => band.severity === 'MIXED');

  const handleChartClick = useCallback((e) => {
    if (isDraggingRef.current) { isDraggingRef.current = false; return; }
    if (!e || !e.activePayload || !e.activePayload.length || !onSelectAlert || !alerts?.length) return;
    const idx = e.activePayload[0]?.payload?._idx;
    if (idx == null) return;
    const band = alarmBands.find((candidate) => idx >= candidate.start && idx <= candidate.end);
    if (!band) return;

    const match = alerts.find((alert) => {
      const alertStart = String(alert.start_ts || '').substring(0, 19);
      const bandStart = String(band.rawStart || '').substring(0, 19);
      const alertEnd = String(alert.end_ts || '').substring(0, 19);
      const bandEnd = String(band.rawEnd || '').substring(0, 19);
      return alert.class === selectedSystem && alertStart === bandStart && alertEnd === bandEnd;
    });
    if (match) {
      onSelectAlert(match);
      return;
    }

    const row = e.activePayload[0]?.payload;
    const pointTs = row?.fullTs ? new Date(String(row.fullTs)).getTime() : null;
    if (pointTs == null || Number.isNaN(pointTs)) return;
    const overlapMatch = alerts.find((alert) => {
      if (alert.class !== selectedSystem) return false;
      const start = new Date(String(alert.start_ts)).getTime();
      const end = new Date(String(alert.end_ts)).getTime();
      return !Number.isNaN(start) && !Number.isNaN(end) && start <= pointTs && pointTs <= end;
    });
    if (overlapMatch) onSelectAlert(overlapMatch);
  }, [alarmBands, alerts, onSelectAlert, selectedSystem]);

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
    if (!chartData.length) return undefined;
    const n = chartData.length;
    const seen = new Set();
    const ticks = [];
    const granularity = n <= 60 ? 'minute' : 'hour';
    for (const row of chartData) {
      const ts = row.fullTs ? String(row.fullTs) : '';
      if (!ts) continue;
      const key = granularity === 'minute' ? ts.substring(11, 16) : ts.substring(11, 13);
      if (!seen.has(key)) { seen.add(key); ticks.push(row._idx); }
    }
    return ticks;
  }, [chartData]);

  const handleZoomMouseDown = useCallback((e) => {
    if (e?.activeLabel != null) {
      setRefAreaLeft(e.activeLabel);
      dragStartRef.current = e.activeLabel;
      isDraggingRef.current = false;
    }
  }, []);

  const handleZoomMouseMove = useCallback((e) => {
    if (refAreaLeft != null && e?.activeLabel != null) {
      setRefAreaRight(e.activeLabel);
      if (dragStartRef.current != null && e.activeLabel !== dragStartRef.current) {
        isDraggingRef.current = true;
      }
    }
  }, [refAreaLeft]);

  const handleZoomMouseUp = useCallback(() => {
    if (refAreaLeft != null && refAreaRight != null && refAreaLeft !== refAreaRight) {
      isDraggingRef.current = true;
      const left = Math.min(refAreaLeft, refAreaRight);
      const right = Math.max(refAreaLeft, refAreaRight);
      if (onZoomChange) {
        const leftRow = chartData.find((r) => r._idx === left) || chartData[left];
        const rightRow = chartData.find((r) => r._idx === right) || chartData[right];
        if (leftRow?.fullTs && rightRow?.fullTs) {
          onZoomChange({ start: leftRow.fullTs, end: rightRow.fullTs });
        }
      }
    }
    setRefAreaLeft(null);
    setRefAreaRight(null);
    dragStartRef.current = null;
  }, [refAreaLeft, refAreaRight, onZoomChange, chartData]);

  return (
    <GlassCard delay={0.5} style={{ marginTop: '8px' }} intensity="strong">
      <div style={styles.heading}>
        Subsystem Behavior
        <InfoTooltip text={alarmView === 'span'
          ? 'Live sensor traces for each subsystem. Gray bands indicate downtime. Colored overlays indicate dynamic contiguous alarm spans built from source minute-level alarms.'
          : 'Live sensor traces for each subsystem. Gray bands indicate downtime. Colored markers indicate source minute-level system alarms.'} />
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

      <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', marginBottom: '10px' }}>
        <div style={controlPillStyle(alarmView === 'minute')} onClick={() => setAlarmView('minute')}>
          Minute Windows
        </div>
        <div style={controlPillStyle(alarmView === 'span')} onClick={() => setAlarmView('span')}>
          Alarm Spans
        </div>
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
              {hasHighAlarms && (
                <div style={{ ...styles.legendItem, cursor: 'default' }}>
                  <div style={{ width: '14px', height: '8px', background: getAlarmStyle('HIGH').swatchBg, borderRadius: '2px', border: getAlarmStyle('HIGH').swatchBorder }} />
                  <span style={{ fontWeight: 600 }}>{alarmView === 'span' ? 'High Span' : 'High Alarm'}</span>
                </div>
              )}
              {hasMediumAlarms && (
                <div style={{ ...styles.legendItem, cursor: 'default' }}>
                  <div style={{ width: '14px', height: '8px', background: getAlarmStyle('MEDIUM').swatchBg, borderRadius: '2px', border: getAlarmStyle('MEDIUM').swatchBorder }} />
                  <span style={{ fontWeight: 600 }}>{alarmView === 'span' ? 'Medium Span' : 'Medium Alarm'}</span>
                </div>
              )}
              {hasLowAlarms && (
                <div style={{ ...styles.legendItem, cursor: 'default' }}>
                  <div style={{ width: '14px', height: '8px', background: getAlarmStyle('LOW').swatchBg, borderRadius: '2px', border: getAlarmStyle('LOW').swatchBorder }} />
                  <span style={{ fontWeight: 600 }}>{alarmView === 'span' ? 'Low Span' : 'Low Alarm'}</span>
                </div>
              )}
              {hasMixedAlarms && (
                <div style={{ ...styles.legendItem, cursor: 'default' }}>
                  <div style={{ width: '14px', height: '8px', background: getAlarmStyle('MIXED').swatchBg, borderRadius: '2px', border: getAlarmStyle('MIXED').swatchBorder }} />
                  <span style={{ fontWeight: 600 }}>Mixed Span</span>
                </div>
              )}
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '6px', marginBottom: '8px' }}>
              <div style={{
                display: 'inline-flex', alignItems: 'center', gap: '4px',
                fontSize: '10px', padding: '4px 12px', borderRadius: '20px',
                border: '1px solid rgba(203,230,200,0.6)', background: 'rgba(255,255,255,0.6)',
                color: '#8A928A', fontWeight: 500,
              }}>
                Drag to zoom
              </div>
              {isZoomed && onZoomReset && (
                <div onClick={onZoomReset} style={{
                  display: 'inline-flex', alignItems: 'center', gap: '6px',
                  fontSize: '10px', padding: '4px 12px', borderRadius: '20px', cursor: 'pointer',
                  border: '1.5px solid rgba(27,94,32,0.4)', background: 'rgba(27,94,32,0.06)',
                  color: '#1B5E20', fontWeight: 600, transition: 'all 0.2s',
                }}>
                  <span>Reset zoom</span>
                  <span style={{ fontSize: '12px', opacity: 0.6 }}>x</span>
                </div>
              )}
            </div>
            <ResponsiveContainer width="100%" height={340}>
              <LineChart
                data={chartData}
                margin={{ top: 10, right: 20, bottom: 5, left: 0 }}
                style={{ cursor: 'crosshair' }}
                onMouseDown={handleZoomMouseDown}
                onMouseMove={handleZoomMouseMove}
                onMouseUp={handleZoomMouseUp}
                onClick={handleChartClick}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(203,230,200,0.4)" />
                <XAxis
                  dataKey="_idx" type="number" domain={['dataMin', 'dataMax']}
                  ticks={xTicks} tick={{ fontSize: 10, fill: '#8A928A' }} tickLine={false}
                  label={{ value: 'UTC', position: 'insideBottomRight', offset: -2, style: { fontSize: 9, fill: '#8A928A' } }}
                  tickFormatter={(idx) => {
                    const row = chartData.find((r) => r._idx === idx) || chartData[idx];
                    if (!row) return '';
                    return row.ts;
                  }}
                />
                <YAxis domain={yDomain} tick={{ fontSize: 10, fill: '#8A928A' }} tickLine={false} />
                <Tooltip content={<CustomTooltip downtimeBands={downtimeBands} alarmBands={alarmBands} alarmView={alarmView} />} />

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

                {/* Alarm bands */}
                {alarmBands.map((band, i) => (
                  (() => {
                    const alarmStyle = getAlarmStyle(band.severity);
                    return (
                  <ReferenceArea
                    key={`alarm-${i}`}
                    x1={band.start} x2={band.end}
                    fill={alarmStyle.areaFill}
                    fillOpacity={1}
                    stroke={alarmStyle.areaStroke}
                    strokeWidth={1.5}
                  />
                    );
                  })()
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

                {/* Drag selection highlight */}
                {refAreaLeft != null && refAreaRight != null && (
                  <ReferenceArea
                    x1={refAreaLeft} x2={refAreaRight}
                    strokeOpacity={0.3}
                    fill="rgba(27,94,32,0.15)"
                  />
                )}
              </LineChart>
            </ResponsiveContainer>



          </>
        )}
      </div>
    </GlassCard>
  );
}

export default SubsystemBehaviorChartBeta;
