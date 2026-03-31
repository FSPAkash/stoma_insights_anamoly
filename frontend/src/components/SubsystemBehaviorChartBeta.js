import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import {
  ResponsiveContainer,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ReferenceArea,
  ReferenceLine,
  ComposedChart,
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
  controlContainer: {
    background: 'rgba(245,248,245,0.6)', border: '1px solid rgba(203,230,200,0.3)',
    borderRadius: '10px', padding: '10px 14px', marginBottom: '12px',
  },
};

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

const parseUtcMs = (value) => {
  if (value == null) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T');
  const ms = Date.parse(normalized);
  return Number.isNaN(ms) ? null : ms;
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
        ? `Anomaly Span -- ${matchedAlarm.severity}`
        : `System Anomaly -- ${matchedAlarm.severity}`,
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

function SubsystemBehaviorChartBeta({ selectedDay, isLatestMode, lastNHours, startTime, endTime, onZoomChange, onZoomReset, isZoomed, onSelectAlert, subsystems: subsystemsProp, allDaysMode, onScrollDayChange, hasData = true, statusByDay = {} }) {
  const alarmView = 'span';
  const regularSubsystems = useMemo(() => (subsystemsProp || []).filter(s => s.system_id !== 'ISOLATED'), [subsystemsProp]);
  const isolatedSubsystems = useMemo(() => (subsystemsProp || []).filter(s => s.system_id === 'ISOLATED'), [subsystemsProp]);
  const subsystems = useMemo(() => [...regularSubsystems, ...isolatedSubsystems], [regularSubsystems, isolatedSubsystems]);
  const [selectedSystem, setSelectedSystem] = useState(null);
  const [sensorData, setSensorData] = useState(null);
  const [alerts, setAlerts] = useState([]);
  const [alertsLoading, setAlertsLoading] = useState(false);
  const [loading, setLoading] = useState(false);
  const [visibleSensors, setVisibleSensors] = useState({});
  const behaviorCacheRef = useRef({});

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
      const res = await getBetaSubsystemBehavior(systemId);
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
    if (!selectedSystem) return;
    let cancelled = false;
    setAlertsLoading(true);
    getBetaAlerts({ alarm_view: 'span', class: selectedSystem })
      .then((res) => {
        if (!cancelled) setAlerts(res.data.alerts || []);
      })
      .catch(() => {
        if (!cancelled) setAlerts([]);
      })
      .finally(() => {
        if (!cancelled) setAlertsLoading(false);
      });
    return () => { cancelled = true; };
  }, [selectedSystem]);

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

    if (!allDaysMode && selectedDay) {
      rows = rows.filter((row) => {
        const ts = row[tsCol];
        return ts && String(ts).substring(0, 10) === selectedDay;
      });
      if (isLatestMode && lastNHours < 24 && rows.length > 0) {
        const lastTs = rows[rows.length - 1][tsCol];
        const endMs = parseUtcMs(lastTs);
        if (endMs != null) {
          const startMs = endMs - lastNHours * 60 * 60 * 1000;
          rows = rows.filter((row) => {
            const rowMs = parseUtcMs(row[tsCol]);
            return rowMs != null && rowMs >= startMs;
          });
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
      _day: row[tsCol] ? String(row[tsCol]).substring(0, 10) : '',
    }));
  }, [sensorData, selectedDay, isLatestMode, lastNHours, startTime, endTime, allDaysMode]);

  const findClosestIdx = useCallback((targetMs, data) => {
    if (!data.length) return null;
    if (targetMs == null || Number.isNaN(targetMs)) return null;
    let best = null;
    let bestDiff = Infinity;
    for (let i = 0; i < data.length; i++) {
      const rowMs = parseUtcMs(data[i].fullTs);
      if (rowMs == null) continue;
      const diff = Math.abs(rowMs - targetMs);
      if (diff < bestDiff) { best = i; bestDiff = diff; }
    }
    return best == null ? null : data[best]._idx;
  }, []);

  const downtimeBands = useMemo(() => {
    if (!sensorData?.downtime_bands?.length || !chartData.length) return [];
    const chartStartMs = parseUtcMs(chartData[0]?.fullTs);
    const chartEndMs = parseUtcMs(chartData[chartData.length - 1]?.fullTs);
    if (chartStartMs == null || chartEndMs == null) return [];
    return sensorData.downtime_bands
      .map((b) => {
        const rawStartMs = parseUtcMs(b.start);
        const rawEndMs = parseUtcMs(b.end ?? b.start);
        if (rawStartMs == null || rawEndMs == null) return null;
        const minMs = Math.min(rawStartMs, rawEndMs);
        const maxMs = Math.max(rawStartMs, rawEndMs);
        if (maxMs < chartStartMs || minMs > chartEndMs) return null;
        const clippedStartMs = Math.max(minMs, chartStartMs);
        const clippedEndMs = Math.min(maxMs, chartEndMs);
        const start = findClosestIdx(clippedStartMs, chartData);
        const end = findClosestIdx(clippedEndMs, chartData);
        if (start == null || end == null) return null;
        if (start === end) return { start: start - 0.45, end: end + 0.45 };
        return { start: Math.min(start, end), end: Math.max(start, end) };
      })
      .filter(Boolean)
      .filter((b) => b.start !== null && b.end !== null);
  }, [sensorData, chartData, findClosestIdx]);

  const alarmBands = useMemo(() => {
    if (!alerts?.length || !chartData.length || !selectedSystem) return [];
    const sysAlerts = alerts.filter((a) => a.class === selectedSystem);
    if (!sysAlerts.length) return [];
    const chartStartMs = parseUtcMs(chartData[0]?.fullTs);
    const chartEndMs = parseUtcMs(chartData[chartData.length - 1]?.fullTs);
    if (chartStartMs == null || chartEndMs == null) return [];
    return sysAlerts
      .map((b) => {
        const rawStartMs = parseUtcMs(b.start_ts || b.event_start);
        const rawEndMs = parseUtcMs(b.end_ts || b.event_end || b.start_ts || b.event_start);
        if (rawStartMs == null || rawEndMs == null) return null;
        const minMs = Math.min(rawStartMs, rawEndMs);
        const maxMs = Math.max(rawStartMs, rawEndMs);
        if (maxMs < chartStartMs || minMs > chartEndMs) return null;
        const clippedStartMs = Math.max(minMs, chartStartMs);
        const clippedEndMs = Math.min(maxMs, chartEndMs);
        const start = findClosestIdx(clippedStartMs, chartData);
        const end = findClosestIdx(clippedEndMs, chartData);
        if (start == null || end == null) return null;
        const band = {
          start: Math.min(start, end),
          end: Math.max(start, end),
          severity: b.severity || 'MEDIUM',
          minute_count: b.minute_count || 1,
          severity_mix: b.severity_mix || '',
          high_count: b.high_count || 0,
          medium_count: b.medium_count || 0,
          low_count: b.low_count || 0,
          view_type: b.view_type || 'span',
          rawStart: b.start_ts || b.event_start,
          rawEnd: b.end_ts || b.event_end || b.start_ts || b.event_start,
        };
        if (band.start === band.end) {
          return { ...band, start: band.start - 0.45, end: band.end + 0.45 };
        }
        return band;
      })
      .filter(Boolean);
  }, [alerts, chartData, selectedSystem, findClosestIdx]);

  const sensors = sensorData?.sensors || [];
  const hasHighAlarms = alarmBands.some((band) => band && band.severity === 'HIGH');
  const hasMediumAlarms = alarmBands.some((band) => band && band.severity === 'MEDIUM');
  const hasLowAlarms = alarmBands.some((band) => band && band.severity === 'LOW');
  const hasMixedAlarms = alarmBands.some((band) => band && band.severity === 'MIXED');

  // Track mouse position to distinguish real clicks from scroll/drag gestures
  const mouseDownPos = useRef(null);
  const handleChartMouseDown = useCallback((evt) => {
    mouseDownPos.current = { x: evt.clientX, y: evt.clientY };
  }, []);
  const handleChartMouseUp = useCallback((evt) => {
    if (!mouseDownPos.current) return;
    const dx = Math.abs(evt.clientX - mouseDownPos.current.x);
    const dy = Math.abs(evt.clientY - mouseDownPos.current.y);
    // If the mouse moved more than 5px, it was a drag/scroll, not a click
    if (dx > 5 || dy > 5) mouseDownPos.current = null;
  }, []);

  const handleChartClick = useCallback((e) => {
    // Reject if mouseDown was cleared by mouseUp (drag/scroll gesture)
    if (!mouseDownPos.current) return;
    mouseDownPos.current = null;
    if (!e || !e.activePayload || !e.activePayload.length) return;
    const idx = e.activePayload[0]?.payload?._idx;
    if (idx == null) return;

    // If clicking an alarm band, select the alert
    const band = alarmBands.find((candidate) => candidate && idx >= candidate.start && idx <= candidate.end);
    if (band && onSelectAlert && alerts?.length) {
      const match = alerts.find((alert) => {
        const alertStart = String(alert.start_ts || '').substring(0, 19);
        const bandStart = String(band.rawStart || '').substring(0, 19);
        const alertEnd = String(alert.end_ts || '').substring(0, 19);
        const bandEnd = String(band.rawEnd || '').substring(0, 19);
        return alert.class === selectedSystem && alertStart === bandStart && alertEnd === bandEnd;
      });
      if (match) { onSelectAlert(match); return; }

      const row = e.activePayload[0]?.payload;
      const pointTs = parseUtcMs(row?.fullTs);
      if (pointTs != null) {
        const overlapMatch = alerts.find((alert) => {
          if (alert.class !== selectedSystem) return false;
          const start = parseUtcMs(alert.start_ts || alert.event_start);
          const end = parseUtcMs(alert.end_ts || alert.event_end || alert.start_ts || alert.event_start);
          return start != null && end != null && start <= pointTs && pointTs <= end;
        });
        if (overlapMatch) { onSelectAlert(overlapMatch); return; }
      }
    }

    // Click-to-zoom: zoom in 2x centered on clicked point
    if (!onZoomChange || !chartData.length) return;
    const n = chartData.length;
    const windowSize = Math.max(Math.floor(n / 2), 10);
    const half = Math.floor(windowSize / 2);
    let left = Math.max(0, idx - half);
    let right = Math.min(n - 1, left + windowSize);
    if (right === n - 1) left = Math.max(0, right - windowSize);
    const leftRow = chartData[left];
    const rightRow = chartData[right];
    if (leftRow?.fullTs && rightRow?.fullTs) {
      const clickedRow = chartData.find(r => r._idx === idx) || chartData[idx];
      onZoomChange({ start: leftRow.fullTs, end: rightRow.fullTs, clickDay: clickedRow?._day });
    }
  }, [alarmBands, alerts, onSelectAlert, selectedSystem, onZoomChange, chartData]);

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
    if (!isFinite(min) || !isFinite(max)) return [0, 0.3];
    const vals = [];
    for (const row of chartData) {
      for (const s of active) {
        const v = row[s];
        if (v != null && isFinite(v)) vals.push(v);
      }
    }
    if (!vals.length) return [0, 0.3];
    vals.sort((a, b) => a - b);
    const p95 = vals[Math.floor(vals.length * 0.95)];
    const hi = Math.ceil(p95 * 1.15);
    return [0, Math.max(hi, 1)];
  }, [chartData, sensors, visibleSensors]);

  // Day boundary indices for vertical separator lines
  const dayBoundaries = useMemo(() => {
    if (!allDaysMode || !chartData.length) return [];
    const boundaries = [];
    const seen = new Set();
    for (const row of chartData) {
      const ts = row.fullTs || '';
      if (!ts) continue;
      const day = ts.substring(0, 10);
      if (!seen.has(day)) {
        seen.add(day);
        boundaries.push({ idx: row._idx, day });
      }
    }
    return boundaries;
  }, [allDaysMode, chartData]);

  const dayLabelMap = useMemo(() => {
    if (!allDaysMode || !dayBoundaries.length) return {};
    const map = {};
    for (const b of dayBoundaries) {
      const d = new Date(b.day + 'T00:00:00');
      map[b.idx] = d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
    }
    return map;
  }, [allDaysMode, dayBoundaries]);

  const xTicks = useMemo(() => {
    if (!chartData.length) return undefined;
    const n = chartData.length;
    const seen = new Set();
    const ticks = [];
    if (allDaysMode) {
      // Show a tick every hour + ensure day boundaries are included
      const dayFirstIdx = new Set(dayBoundaries.map((b) => b.idx));
      for (const row of chartData) {
        const ts = row.fullTs || '';
        if (!ts) continue;
        const key = ts.substring(0, 13);
        if (!seen.has(key) || dayFirstIdx.has(row._idx)) {
          seen.add(key);
          if (!ticks.includes(row._idx)) ticks.push(row._idx);
        }
      }
    } else {
      const granularity = n <= 60 ? 'minute' : 'hour';
      for (const row of chartData) {
        const ts = row.fullTs ? String(row.fullTs) : '';
        if (!ts) continue;
        const key = granularity === 'minute' ? ts.substring(11, 16) : ts.substring(11, 13);
        if (!seen.has(key)) { seen.add(key); ticks.push(row._idx); }
      }
    }
    return ticks;
  }, [chartData, allDaysMode, dayBoundaries]);

  // Scroll infrastructure for allDaysMode
  const scrollContainerRef = useRef(null);
  const [visibleDay, setVisibleDay] = useState(null);

  const scrollChartWidth = useMemo(() => {
    if (!allDaysMode || !chartData.length) return null;
    const days = new Set(chartData.map(r => r._day).filter(Boolean));
    const numDays = Math.max(days.size, 1);
    const pointsPerDay = chartData.length / numDays;
    const viewportWidth = 1200;
    const pxPerPoint = viewportWidth / pointsPerDay;
    return Math.max(Math.round(chartData.length * pxPerPoint), 1200);
  }, [allDaysMode, chartData]);

  const handleChartScroll = useCallback((e) => {
    if (!allDaysMode || !chartData.length || !onScrollDayChange) return;
    const container = e.target;
    const scrollLeft = container.scrollLeft;
    const viewWidth = container.clientWidth;
    const totalWidth = container.scrollWidth;
    const centerFraction = (scrollLeft + viewWidth / 2) / totalWidth;
    const centerIdx = Math.floor(centerFraction * chartData.length);
    const row = chartData[Math.min(centerIdx, chartData.length - 1)];
    if (row?._day && row._day !== visibleDay) {
      setVisibleDay(row._day);
      onScrollDayChange(row._day);
    }
  }, [allDaysMode, chartData, visibleDay, onScrollDayChange]);

  useEffect(() => {
    if (allDaysMode && scrollContainerRef.current) {
      const el = scrollContainerRef.current;
      setTimeout(() => { el.scrollLeft = el.scrollWidth - el.clientWidth; }, 50);
    }
  }, [allDaysMode, scrollChartWidth]);


  if (!hasData) {
    return (
      <GlassCard delay={0.5} style={{ marginTop: '8px' }} intensity="strong">
        <div style={styles.heading}>Subsystem Behavior</div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '300px', color: '#8A928A', fontSize: '15px' }}>
          No data available for the selected system
        </div>
      </GlassCard>
    );
  }

  return (
    <GlassCard delay={0.5} style={{ marginTop: '8px' }} intensity="strong">
      <div style={styles.heading}>
        Subsystem Behavior
        <InfoTooltip text="Live sensor traces for each subsystem. Gray bands indicate downtime. Colored overlays indicate dynamic contiguous anomaly spans built from source minute-level anomalies." />
      </div>

      <div style={styles.tabRow}>
        {regularSubsystems.map((sys, idx) => {
          const color = systemColor(sys.system_id, idx);
          return (
            <div key={sys.system_id} style={styles.tab(selectedSystem === sys.system_id, color)}
              onClick={() => setSelectedSystem(sys.system_id)}>
              {sys.system_id.replace('_', ' ')} ({sys.sensor_count})
            </div>
          );
        })}
        {isolatedSubsystems.length > 0 && (
          <>
            <div style={{ width: '1px', background: 'rgba(120,120,120,0.25)', margin: '2px 4px', alignSelf: 'stretch' }} />
            {isolatedSubsystems.map((sys) => {
              const color = '#757575';
              return (
                <div key={sys.system_id} style={styles.tab(selectedSystem === sys.system_id, color)}
                  onClick={() => setSelectedSystem(sys.system_id)}>
                  Isolated ({sys.sensor_count})
                </div>
              );
            })}
          </>
        )}
      </div>

      <div style={{ ...styles.chartContainer, position: 'relative' }}>
        {alertsLoading && !loading && (
          <div style={{
            position: 'absolute', top: 0, left: 0, right: 0, bottom: 0,
            background: 'rgba(20, 24, 20, 0.45)', backdropFilter: 'blur(2px)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            zIndex: 10, borderRadius: '8px',
            color: '#C8D6C0', fontSize: '13px', fontWeight: 500, letterSpacing: '0.3px',
          }}>
            Updating anomaly overlays...
          </div>
        )}
        {loading ? (
          <div style={styles.loadingOverlay}>Loading sensor data...</div>
        ) : !chartData.length ? (
          <div style={styles.emptyState}>
            {selectedSystem ? `No sensor data for ${selectedSystem}.` : 'Select a subsystem above.'}
          </div>
        ) : (
          <>
            {/* Sensor toggle legend */}
            <div style={{ ...styles.controlContainer, marginBottom: '12px' }}>
            <div style={{ ...styles.legendRow, marginTop: 0 }}>
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
                  <span style={{ fontWeight: 600 }}>High Span</span>
                </div>
              )}
              {hasMediumAlarms && (
                <div style={{ ...styles.legendItem, cursor: 'default' }}>
                  <div style={{ width: '14px', height: '8px', background: getAlarmStyle('MEDIUM').swatchBg, borderRadius: '2px', border: getAlarmStyle('MEDIUM').swatchBorder }} />
                  <span style={{ fontWeight: 600 }}>Medium Span</span>
                </div>
              )}
              {hasLowAlarms && (
                <div style={{ ...styles.legendItem, cursor: 'default' }}>
                  <div style={{ width: '14px', height: '8px', background: getAlarmStyle('LOW').swatchBg, borderRadius: '2px', border: getAlarmStyle('LOW').swatchBorder }} />
                  <span style={{ fontWeight: 600 }}>Low Span</span>
                </div>
              )}
              {hasMixedAlarms && (
                <div style={{ ...styles.legendItem, cursor: 'default' }}>
                  <div style={{ width: '14px', height: '8px', background: getAlarmStyle('MIXED').swatchBg, borderRadius: '2px', border: getAlarmStyle('MIXED').swatchBorder }} />
                  <span style={{ fontWeight: 600 }}>Mixed Span</span>
                </div>
              )}
              <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: '8px' }}>
                {!isZoomed && (
                  <div style={{ display: 'inline-flex', alignItems: 'center', gap: '4px', fontSize: '9px', color: '#8A928A', opacity: 0.7 }}>
                    <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="#8A928A" strokeWidth="2"><circle cx="6.5" cy="6.5" r="4.5"/><line x1="10" y1="10" x2="14" y2="14"/></svg>
                    <span>Click to zoom</span>
                  </div>
                )}
                {isZoomed && onZoomReset && (
                  <div onClick={onZoomReset} style={{
                    display: 'inline-flex', alignItems: 'center', gap: '5px',
                    fontSize: '10px', padding: '3px 10px', borderRadius: '14px', cursor: 'pointer',
                    border: '1.5px solid rgba(27,94,32,0.4)', background: 'rgba(27,94,32,0.06)',
                    color: '#1B5E20', fontWeight: 600, transition: 'all 0.2s',
                  }}>
                    <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="#1B5E20" strokeWidth="2"><circle cx="6.5" cy="6.5" r="4.5"/><line x1="10" y1="10" x2="14" y2="14"/><line x1="4.5" y1="6.5" x2="8.5" y2="6.5"/></svg>
                    <span>Reset zoom</span>
                  </div>
                )}
              </div>
            </div>
            </div>

            <div ref={scrollContainerRef} onScroll={handleChartScroll}
              style={allDaysMode ? { overflowX: 'auto', overflowY: 'hidden', width: '100%' } : {}}>
            <div style={allDaysMode ? { width: `${scrollChartWidth}px`, minWidth: '100%' } : {}}>
            {(() => {
              const displayDay = allDaysMode ? visibleDay : selectedDay;
              if (!displayDay) return null;
              return (
              <div style={{
                position: 'sticky',
                left: 0,
                zIndex: 2,
                width: 'fit-content',
                marginBottom: '6px',
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
              }}>
                <div style={{
                  padding: '4px 12px',
                  fontSize: '11px',
                  fontWeight: 600,
                  color: '#1B5E20',
                  background: 'rgba(245,248,245,0.9)',
                  border: '1px solid rgba(203,230,200,0.5)',
                  borderRadius: '6px',
                }}>
                  {new Date(displayDay + 'T00:00:00').toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' })}
                </div>
                {statusByDay?.[displayDay] && (
                  <div
                    title={statusByDay[displayDay].detail}
                    style={{
                      padding: '3px 10px',
                      fontSize: '11px',
                      fontWeight: 600,
                      color: statusByDay[displayDay].color,
                      background: `${statusByDay[displayDay].color}18`,
                      borderRadius: '999px',
                      border: `1px solid ${statusByDay[displayDay].color}55`,
                    }}
                  >
                    Status: {statusByDay[displayDay].label}
                  </div>
                )}
              </div>
              );
            })()}
            <div onMouseDown={handleChartMouseDown} onMouseUp={handleChartMouseUp}>
            <ResponsiveContainer width="100%" height={340}>
              <ComposedChart
                data={chartData}
                margin={{ top: 10, right: 20, bottom: 5, left: 0 }}
                style={{ cursor: 'zoom-in' }}
                onClick={handleChartClick}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(203,230,200,0.4)" />
                {dayBoundaries.map((b) => (
                  <ReferenceLine key={`day-${b.day}`} x={b.idx} stroke="#F9A825" strokeWidth={2} strokeDasharray="8 4"
                    label={{ value: dayLabelMap[b.idx] || '', position: 'insideTopLeft', style: { fontSize: 11, fontWeight: 700, fill: '#F9A825' }, offset: 4 }}
                  />
                ))}
                <XAxis
                  dataKey="_idx" type="number" domain={['dataMin', 'dataMax']}
                  ticks={xTicks} tickLine={false}
                  label={{ value: 'UTC', position: 'insideBottomRight', offset: -2, style: { fontSize: 9, fill: '#8A928A' } }}
                  tick={(props) => {
                    const { x, y, payload } = props;
                    const idx = payload.value;
                    const row = chartData.find((r) => r._idx === idx) || chartData[idx];
                    if (!row) return null;
                    let label = row.ts;
                    let isDayLabel = false;
                    if (allDaysMode && row.fullTs) {
                      label = row.fullTs.substring(11, 16);
                    }
                    return (
                      <g>
                        <text x={x} y={y + 12} textAnchor="middle"
                          fontSize={isDayLabel ? 11 : 9}
                          fontWeight={isDayLabel ? 700 : 400}
                          fill={isDayLabel ? '#F9A825' : '#8A928A'}
                        >{label}</text>
                      </g>
                    );
                  }}
                />
                <YAxis domain={yDomain} allowDataOverflow={true} tick={{ fontSize: 10, fill: '#8A928A' }} tickLine={false} />
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
                      strokeWidth={1}
                      dot={false}
                      name={formatSensorName(s)}
                      animationDuration={600}
                      connectNulls={false}
                    />
                  ) : null
                )}

              </ComposedChart>
            </ResponsiveContainer>
            </div>
            </div>
            </div>



          </>
        )}
      </div>
    </GlassCard>
  );
}

export default SubsystemBehaviorChartBeta;
