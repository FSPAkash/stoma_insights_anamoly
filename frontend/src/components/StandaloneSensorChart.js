import { useState, useEffect, useMemo, useCallback, useRef } from 'react';
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
import { getBetaStandaloneSensors, getBetaStandaloneSensor } from '../utils/api';
import { formatSensorName } from '../utils/formatters';

const METRIC_OPTIONS = [
  { key: 'evidence', label: 'Sensor Score', color: '#1B5E20' },
  { key: 'threshold', label: 'Threshold', color: '#E65100', dash: '6 3' },
  { key: 'sqs', label: 'SQS', color: '#1976D2' },
  { key: 'engine_a', label: 'Engine A', color: '#E65100' },
  { key: 'engine_b', label: 'Engine B', color: '#7B1FA2' },
];

const DEFAULT_VISIBLE = { evidence: true, threshold: true, sqs: true, engine_a: true, engine_b: true };

const getAlarmStyle = (severity) => (
  severity === 'HIGH'
    ? { text: '#D32F2F', bg: 'rgba(239,83,80,0.10)', border: '1px solid rgba(239,83,80,0.3)', swatchBg: 'rgba(239,83,80,0.30)', swatchBorder: '1px solid rgba(239,83,80,0.5)', areaFill: 'rgba(239,83,80,0.22)', areaStroke: 'rgba(239,83,80,0.5)' }
    : severity === 'MEDIUM'
      ? { text: '#E65100', bg: 'rgba(255,167,38,0.14)', border: '1px solid rgba(255,167,38,0.32)', swatchBg: 'rgba(255,167,38,0.34)', swatchBorder: '1px solid rgba(245,124,0,0.45)', areaFill: 'rgba(255,167,38,0.20)', areaStroke: 'rgba(245,124,0,0.46)' }
      : severity === 'MIXED'
        ? { text: '#455A64', bg: 'rgba(120,144,156,0.16)', border: '1px solid rgba(120,144,156,0.32)', swatchBg: 'rgba(120,144,156,0.30)', swatchBorder: '1px solid rgba(84,110,122,0.45)', areaFill: 'rgba(120,144,156,0.20)', areaStroke: 'rgba(84,110,122,0.46)' }
        : { text: '#9A6A00', bg: 'rgba(255,213,79,0.18)', border: '1px solid rgba(255,193,7,0.34)', swatchBg: 'rgba(255,213,79,0.36)', swatchBorder: '1px solid rgba(255,179,0,0.45)', areaFill: 'rgba(255,213,79,0.22)', areaStroke: 'rgba(255,179,0,0.46)' }
);

const parseUtcMs = (value) => {
  if (value == null) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T');
  const ms = Date.parse(normalized);
  return Number.isNaN(ms) ? null : ms;
};

const styles = {
  heading: {
    fontSize: '12px', fontWeight: 500, color: '#6B736B', textTransform: 'uppercase',
    letterSpacing: '0.05em', marginBottom: '16px', display: 'flex', alignItems: 'center',
  },
  controlContainer: {
    background: 'rgba(245,248,245,0.6)', border: '1px solid rgba(203,230,200,0.3)',
    borderRadius: '10px', padding: '10px 14px', marginBottom: '12px',
  },
  chartContainer: { position: 'relative', minHeight: '340px', overflow: 'visible' },
  loadingOverlay: {
    display: 'flex', alignItems: 'center', justifyContent: 'center',
    height: '300px', color: '#8A928A', fontSize: '13px',
  },
  legendRow: {
    display: 'flex', gap: '6px', flexWrap: 'wrap', marginTop: '0', justifyContent: 'center',
  },
  legendItem: {
    display: 'flex', alignItems: 'center', gap: '4px', fontSize: '10px', color: '#6B736B',
  },
  emptyState: { textAlign: 'center', padding: '48px 20px', color: '#8A928A', fontSize: '14px' },
  metricBtn: (active, color) => ({
    padding: '4px 12px', borderRadius: '12px', fontSize: '11px', fontWeight: active ? 600 : 400,
    cursor: 'pointer', border: active ? `1.5px solid ${color}` : '1px solid rgba(203,230,200,0.4)',
    background: active ? `${color}14` : 'transparent', color: active ? color : '#8A928A',
    transition: 'all 0.2s',
  }),
};

const StandaloneTooltip = ({ active, payload, downtimeBands, alarmBands }) => {
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
    const as = getAlarmStyle(matchedAlarm.severity);
    zoneLabels.push({ text: `Alarm Span - ${matchedAlarm.severity}`, color: as.text, bg: as.bg, border: as.border });
  }

  // Score level badge
  const scoreLevel = row?.score_level;

  return (
    <div style={{
      background: 'rgba(255,255,255,0.94)', backdropFilter: 'blur(20px)',
      border: '1px solid rgba(203,230,200,0.5)', borderRadius: '14px',
      padding: '12px 16px', boxShadow: '0 12px 36px rgba(27,94,32,0.12)',
      fontSize: '11px', maxWidth: '420px',
    }}>
      {zoneLabels.map((zl, zi) => (
        <div style={{
          background: zl.bg, border: zl.border,
          borderRadius: '6px', padding: '4px 10px', marginBottom: '4px',
          fontSize: '10px', fontWeight: 700, color: zl.color, letterSpacing: '0.04em',
        }} key={zi}>{zl.text}</div>
      ))}
      <div style={{ fontWeight: 600, color: '#1B5E20', marginBottom: '6px' }}>{displayTs}</div>
      {scoreLevel && (
        <div style={{
          display: 'inline-block', fontSize: '9px', fontWeight: 700, letterSpacing: '0.04em',
          padding: '2px 8px', borderRadius: '4px', marginBottom: '6px',
          background: scoreLevel === 'High' ? 'rgba(239,83,80,0.12)' : scoreLevel === 'Medium' ? 'rgba(255,167,38,0.14)' : 'rgba(76,175,80,0.12)',
          color: scoreLevel === 'High' ? '#D32F2F' : scoreLevel === 'Medium' ? '#E65100' : '#1B5E20',
          border: `1px solid ${scoreLevel === 'High' ? 'rgba(239,83,80,0.3)' : scoreLevel === 'Medium' ? 'rgba(255,167,38,0.3)' : 'rgba(76,175,80,0.3)'}`,
        }}>
          {scoreLevel} Score Level
        </div>
      )}
      {payload.map((p, i) => (
        <div key={i} style={{ display: 'flex', justifyContent: 'space-between', gap: '16px', marginBottom: '2px' }}>
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

function StandaloneSensorChart({ selectedDay, isLatestMode, lastNHours, startTime, endTime, allDaysMode, onScrollDayChange, onZoomChange, onZoomReset, isZoomed, onSelectAlert, hasData = true, statusByDay = {} }) {
  const [sensors, setSensors] = useState([]);
  const [selectedSensor, setSelectedSensor] = useState(null);
  const [sensorData, setSensorData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [sensorsLoading, setSensorsLoading] = useState(true);
  const [visibleMetrics, setVisibleMetrics] = useState({ ...DEFAULT_VISIBLE });
  const [showAlarms, setShowAlarms] = useState(true);
  const [dropdownOpen, setDropdownOpen] = useState(false);

  const dataCacheRef = useRef({});
  const scrollContainerRef = useRef(null);
  const [visibleDay, setVisibleDay] = useState(null);

  // Load sensor list
  useEffect(() => {
    setSensorsLoading(true);
    getBetaStandaloneSensors()
      .then((res) => {
        const list = res.data.sensors || [];
        setSensors(list);
      })
      .catch(() => setSensors([]))
      .finally(() => setSensorsLoading(false));
  }, []);

  // Load sensor data
  const loadSensor = useCallback(async (sensor) => {
    if (!sensor) return;
    if (dataCacheRef.current[sensor]) {
      setSensorData(dataCacheRef.current[sensor]);
      return;
    }
    setLoading(true);
    try {
      const res = await getBetaStandaloneSensor(sensor);
      dataCacheRef.current[sensor] = res.data;
      setSensorData(res.data);
    } catch {
      setSensorData(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (selectedSensor) loadSensor(selectedSensor);
  }, [selectedSensor, loadSensor]);

  const toggleMetric = (key) => {
    setVisibleMetrics((prev) => {
      const activeCount = Object.values(prev).filter(Boolean).length;
      if (prev[key] && activeCount <= 1) return prev;
      return { ...prev, [key]: !prev[key] };
    });
  };

  const applyTimeFilter = useCallback((rows, tsKey = 'ts') => {
    if (!rows.length) return rows;
    if (allDaysMode) return rows;
    if (!selectedDay) return rows;
    let filtered = rows.filter((row) => {
      const ts = row[tsKey];
      return ts && String(ts).substring(0, 10) === selectedDay;
    });
    if (isLatestMode && lastNHours < 24 && filtered.length > 0) {
      const lastTs = filtered[filtered.length - 1][tsKey];
      const endMs = parseUtcMs(lastTs);
      if (endMs != null) {
        const startMs = endMs - lastNHours * 60 * 60 * 1000;
        filtered = filtered.filter((row) => {
          const rowMs = parseUtcMs(row[tsKey]);
          return rowMs != null && rowMs >= startMs;
        });
      }
    } else if (!isLatestMode) {
      filtered = filtered.filter((row) => {
        const hhmm = String(row[tsKey]).substring(11, 16);
        return hhmm >= startTime && hhmm <= endTime;
      });
    }
    return filtered;
  }, [selectedDay, isLatestMode, lastNHours, startTime, endTime, allDaysMode]);

  const chartData = useMemo(() => {
    if (!sensorData?.timeseries?.length) return [];
    const filtered = applyTimeFilter(sensorData.timeseries);
    return filtered.map((row, i) => {
      const full = row.ts ? String(row.ts) : '';
      return {
        ...row, _idx: i,
        fullTs: full,
        ts: full ? full.substring(11, 16) : '',
        _day: full ? full.substring(0, 10) : '',
      };
    });
  }, [sensorData, applyTimeFilter]);

  const findClosestIdx = useCallback((targetMs, rows) => {
    if (!rows.length) return null;
    if (targetMs == null || Number.isNaN(targetMs)) return null;
    let best = null;
    let bestDiff = Infinity;
    for (let i = 0; i < rows.length; i++) {
      const rowMs = parseUtcMs(rows[i].fullTs);
      if (rowMs == null) continue;
      const diff = Math.abs(rowMs - targetMs);
      if (diff < bestDiff) { best = i; bestDiff = diff; }
    }
    return best == null ? null : rows[best]._idx;
  }, []);

  // Downtime bands
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
        const start = findClosestIdx(Math.max(minMs, chartStartMs), chartData);
        const end = findClosestIdx(Math.min(maxMs, chartEndMs), chartData);
        if (start == null || end == null) return null;
        if (start === end) return { start: start - 0.45, end: end + 0.45 };
        return { start: Math.min(start, end), end: Math.max(start, end) };
      })
      .filter(Boolean);
  }, [sensorData, chartData, findClosestIdx]);

  // Inline downtime fallback
  const inlineDowntimeBands = useMemo(() => {
    if (downtimeBands.length > 0) return [];
    if (!chartData.length) return [];
    const bands = [];
    let start = null;
    for (let i = 0; i < chartData.length; i++) {
      if (chartData[i].downtime === 1) { if (start === null) start = i; }
      else { if (start !== null) { bands.push({ start, end: i - 1 }); start = null; } }
    }
    if (start !== null) bands.push({ start, end: chartData.length - 1 });
    return bands;
  }, [chartData, downtimeBands]);

  const allDowntimeBands = downtimeBands.length > 0 ? downtimeBands : inlineDowntimeBands;

  // Alarm bands
  const alarmBands = useMemo(() => {
    if (!sensorData?.alarm_bands?.length || !chartData.length) return [];
    const chartStartMs = parseUtcMs(chartData[0]?.fullTs);
    const chartEndMs = parseUtcMs(chartData[chartData.length - 1]?.fullTs);
    if (chartStartMs == null || chartEndMs == null) return [];
    return sensorData.alarm_bands
      .map((b) => {
        const rawStartMs = parseUtcMs(b.start);
        const rawEndMs = parseUtcMs(b.end ?? b.start);
        if (rawStartMs == null || rawEndMs == null) return null;
        const minMs = Math.min(rawStartMs, rawEndMs);
        const maxMs = Math.max(rawStartMs, rawEndMs);
        if (maxMs < chartStartMs || minMs > chartEndMs) return null;
        const start = findClosestIdx(Math.max(minMs, chartStartMs), chartData);
        const end = findClosestIdx(Math.min(maxMs, chartEndMs), chartData);
        if (start == null || end == null) return null;
        const band = {
          start: Math.min(start, end), end: Math.max(start, end),
          severity: b.severity || 'MEDIUM', minute_count: b.minute_count || 1,
          high_count: b.high_count || 0, medium_count: b.medium_count || 0, low_count: b.low_count || 0,
          severity_mix: b.severity_mix || '', view_type: b.view_type || 'span',
          rawStart: b.start, rawEnd: b.end, mean_trust: b.mean_trust ?? null, peak_evidence: b.peak_evidence ?? null,
        };
        if (band.start === band.end) return { ...band, start: band.start - 0.45, end: band.end + 0.45 };
        return band;
      })
      .filter(Boolean);
  }, [sensorData, chartData, findClosestIdx]);

  const visibleAlarmBands = showAlarms ? alarmBands : [];
  const hasHighAlarms = visibleAlarmBands.some((b) => b && b.severity === 'HIGH');
  const hasMediumAlarms = visibleAlarmBands.some((b) => b && b.severity === 'MEDIUM');
  const hasLowAlarms = visibleAlarmBands.some((b) => b && b.severity === 'LOW');
  const hasMixedAlarms = visibleAlarmBands.some((b) => b && b.severity === 'MIXED');

  // Visible chart data for y-domain
  const visibleChartData = useMemo(() => {
    if (!allDaysMode || !visibleDay) return chartData;
    return chartData.filter(r => r._day === visibleDay);
  }, [chartData, allDaysMode, visibleDay]);

  // Y-domain: dynamic based on visible metric values
  const yDomain = useMemo(() => {
    if (!visibleChartData.length) return [0, 0.5];
    const vals = [];
    for (const row of visibleChartData) {
      for (const m of METRIC_OPTIONS) {
        if (visibleMetrics[m.key]) {
          const v = row[m.key];
          if (v != null && isFinite(v)) vals.push(v);
        }
      }
    }
    if (!vals.length) return [0, 0.5];
    vals.sort((a, b) => a - b);
    const p95 = vals[Math.floor(vals.length * 0.95)];
    const hi = Math.min(1.2, Math.ceil((p95 * 1.3) * 100) / 100);
    return [0, Math.max(hi, 0.15)];
  }, [visibleChartData, visibleMetrics]);

  const yTicks = useMemo(() => {
    const max = yDomain[1];
    if (max <= 0) return [0];
    const count = 5;
    const ticks = [];
    for (let i = 0; i <= count; i++) ticks.push(Math.round((max * i / count) * 1000) / 1000);
    return ticks;
  }, [yDomain]);

  // Day boundaries
  const dayBoundaries = useMemo(() => {
    if (!allDaysMode || !chartData.length) return [];
    const boundaries = [];
    const seen = new Set();
    for (const row of chartData) {
      const ts = row.fullTs || '';
      if (!ts) continue;
      const day = ts.substring(0, 10);
      if (!seen.has(day)) { seen.add(day); boundaries.push({ idx: row._idx, day }); }
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

  // X-axis ticks
  const xTicks = useMemo(() => {
    if (!chartData.length) return [];
    const seen = new Set();
    const ticks = [];
    if (allDaysMode) {
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
      const n = chartData.length;
      const granularity = n <= 60 ? 'minute' : 'hour';
      for (const row of chartData) {
        const ts = row.fullTs || '';
        if (!ts) continue;
        const key = granularity === 'minute' ? ts.substring(11, 16) : ts.substring(11, 13);
        if (!seen.has(key)) { seen.add(key); ticks.push(row._idx); }
      }
    }
    return ticks;
  }, [chartData, allDaysMode, dayBoundaries]);

  // Scrollable chart width
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
    const centerFraction = (container.scrollLeft + container.clientWidth / 2) / container.scrollWidth;
    const centerIdx = Math.floor(centerFraction * chartData.length);
    const row = chartData[Math.min(centerIdx, chartData.length - 1)];
    if (row?._day && row._day !== visibleDay) {
      setVisibleDay(row._day);
      onScrollDayChange(row._day);
    }
  }, [allDaysMode, chartData, onScrollDayChange, visibleDay]);

  // Initialize visibleDay from data when entering allDaysMode
  useEffect(() => {
    if (allDaysMode && chartData.length) {
      const lastRow = chartData[chartData.length - 1];
      if (lastRow?._day && !visibleDay) {
        setVisibleDay(lastRow._day);
      }
    }
  }, [allDaysMode, chartData, visibleDay]);

  useEffect(() => {
    if (allDaysMode && scrollContainerRef.current) {
      const el = scrollContainerRef.current;
      setTimeout(() => { el.scrollLeft = el.scrollWidth; }, 100);
    }
  }, [allDaysMode, chartData.length]);

  // Click-to-zoom
  const mouseDownPos = useRef(null);
  const handleChartMouseDown = useCallback((evt) => { mouseDownPos.current = { x: evt.clientX, y: evt.clientY }; }, []);
  const handleChartMouseUp = useCallback((evt) => {
    if (!mouseDownPos.current) return;
    const dx = Math.abs(evt.clientX - mouseDownPos.current.x);
    const dy = Math.abs(evt.clientY - mouseDownPos.current.y);
    if (dx > 5 || dy > 5) mouseDownPos.current = null;
  }, []);
  // Convert an alarm band into the alert shape BetaAlertDetailModal expects
  const bandToAlert = useCallback((band) => ({
    start_ts: band.rawStart,
    end_ts: band.rawEnd,
    class: selectedSensor || '',
    severity: band.severity,
    minute_count: band.minute_count,
    high_count: band.high_count || 0,
    medium_count: band.medium_count || 0,
    low_count: band.low_count || 0,
    severity_mix: band.severity_mix || '',
    view_type: band.view_type || 'span',
    duration_minutes: band.minute_count,
    system_confidence: band.mean_trust ?? null,
    peak_risk_score: band.peak_evidence ?? null,
    is_standalone: true,
  }), [selectedSensor]);

  const handleChartClick = useCallback((e) => {
    if (!mouseDownPos.current) return;
    mouseDownPos.current = null;
    if (!e || !e.activePayload || !e.activePayload.length) return;
    const idx = e.activePayload[0]?.payload?._idx;
    if (idx == null) return;

    // If clicking an alarm band, open the detail modal
    if (showAlarms && onSelectAlert) {
      const band = visibleAlarmBands.find((b) => b && idx >= b.start && idx <= b.end);
      if (band) { onSelectAlert(bandToAlert(band)); return; }
    }

    // Click-to-zoom
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
      const clickedRow = chartData[idx];
      onZoomChange({ start: leftRow.fullTs, end: rightRow.fullTs, clickDay: clickedRow?._day });
    }
  }, [showAlarms, onSelectAlert, visibleAlarmBands, bandToAlert, onZoomChange, chartData]);

  // Sensor metadata for selected sensor
  const sensorMeta = useMemo(() => sensors.find(s => s.Sensor === selectedSensor), [sensors, selectedSensor]);

  if (!hasData) {
    return (
      <GlassCard delay={0.5} style={{ marginTop: '8px' }} intensity="strong">
        <div style={styles.heading}>Standalone Sensor Analysis</div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '300px', color: '#8A928A', fontSize: '15px' }}>
          No data available for the selected system
        </div>
      </GlassCard>
    );
  }

  return (
    <GlassCard delay={0.5} style={{ marginTop: '8px' }} intensity="strong">
      <div style={styles.heading}>
        Standalone Sensor Analysis
        <InfoTooltip text="Per-sensor score, SQS, Engine A/B scores, threshold, and trust from the standalone analysis pipeline. Select a sensor from the dropdown to view its individual quality profile with anomaly and downtime overlays." />
      </div>

      {/* Sensor dropdown */}
      <div style={{ ...styles.controlContainer, display: 'flex', alignItems: 'center', gap: '12px', flexWrap: 'wrap' }}>
        <div style={{ fontSize: '11px', fontWeight: 600, color: '#6B736B' }}>Sensor:</div>
        <div style={{ position: 'relative' }}>
          <div
            onClick={() => setDropdownOpen(!dropdownOpen)}
            style={{
              padding: '6px 32px 6px 14px', borderRadius: '10px', fontSize: '12px',
              fontWeight: 600, cursor: 'pointer', minWidth: '200px',
              border: '1.5px solid rgba(27,94,32,0.3)', background: 'rgba(27,94,32,0.04)',
              color: '#1B5E20', userSelect: 'none', position: 'relative',
            }}
          >
            {selectedSensor ? formatSensorName(selectedSensor) : 'Select sensor...'}
            <span style={{ position: 'absolute', right: '10px', top: '50%', transform: `translateY(-50%) rotate(${dropdownOpen ? 180 : 0}deg)`, transition: 'transform 0.2s', fontSize: '10px' }}>&#9660;</span>
          </div>
          {dropdownOpen && (
            <div style={{
              position: 'absolute', top: '100%', left: 0, right: 0, marginTop: '4px',
              background: 'rgba(255,255,255,0.97)', backdropFilter: 'blur(20px)',
              border: '1px solid rgba(203,230,200,0.5)', borderRadius: '10px',
              boxShadow: '0 8px 24px rgba(27,94,32,0.12)', zIndex: 100,
              maxHeight: '300px', overflowY: 'auto', minWidth: '260px',
            }}>
              {sensors.map((s) => (
                <div key={s.Sensor}
                  onClick={() => { setSelectedSensor(s.Sensor); setDropdownOpen(false); }}
                  style={{
                    padding: '8px 14px', fontSize: '11px', cursor: 'pointer',
                    fontWeight: selectedSensor === s.Sensor ? 700 : 400,
                    color: selectedSensor === s.Sensor ? '#1B5E20' : '#6B736B',
                    background: selectedSensor === s.Sensor ? 'rgba(27,94,32,0.06)' : 'transparent',
                    borderBottom: '1px solid rgba(203,230,200,0.2)',
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                  }}
                >
                  <span>{formatSensorName(s.Sensor)}</span>
                  <span style={{ fontSize: '9px', color: '#8A928A' }}>{s.AE_Subsystem}</span>
                </div>
              ))}
            </div>
          )}
        </div>
        {sensorMeta && (
          <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
            <span style={{ fontSize: '10px', padding: '3px 10px', borderRadius: '12px', background: 'rgba(27,94,32,0.06)', border: '1px solid rgba(27,94,32,0.15)', color: '#1B5E20', fontWeight: 600 }}>
              {sensorMeta.AE_Subsystem}
            </span>
            {sensorMeta.Alarm_Count > 0 && (
              <span style={{ fontSize: '10px', padding: '3px 10px', borderRadius: '12px', background: 'rgba(239,83,80,0.08)', border: '1px solid rgba(239,83,80,0.2)', color: '#D32F2F', fontWeight: 600 }}>
                {sensorMeta.Alarm_Count} alarms
              </span>
            )}
            {sensorMeta.High_Pct && (
              <span style={{ fontSize: '10px', padding: '3px 10px', borderRadius: '12px', background: 'rgba(255,167,38,0.1)', border: '1px solid rgba(255,167,38,0.25)', color: '#E65100', fontWeight: 600 }}>
                High: {sensorMeta.High_Pct}
              </span>
            )}
          </div>
        )}
      </div>

      {/* Metric toggles */}
      <div style={{ ...styles.controlContainer, display: 'flex', gap: '6px', justifyContent: 'center', flexWrap: 'wrap', alignItems: 'center' }}>
        {METRIC_OPTIONS.map(m => (
          <div key={m.key} style={styles.metricBtn(visibleMetrics[m.key], m.color)}
            onClick={() => toggleMetric(m.key)}>
            {m.label}
          </div>
        ))}
        <div style={{ width: '1px', height: '20px', background: 'rgba(176,205,174,0.7)', margin: '0 4px' }} />
        <div
          onClick={() => setShowAlarms((v) => !v)}
          style={{
            display: 'inline-flex', alignItems: 'center', gap: '6px',
            fontSize: '10px', padding: '4px 12px', borderRadius: '20px', cursor: 'pointer',
            border: showAlarms ? '1.5px solid rgba(230,81,0,0.34)' : '1px solid rgba(203,230,200,0.6)',
            background: showAlarms ? 'rgba(255,167,38,0.12)' : 'rgba(255,255,255,0.6)',
            color: showAlarms ? '#9A6A00' : '#8A928A', fontWeight: 600, transition: 'all 0.2s',
          }}
        >
          <span>Show Anomalies</span>
        </div>
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

      <div style={{ ...styles.chartContainer, position: 'relative' }}>
        {sensorsLoading || loading ? (
          <div style={styles.loadingOverlay}>Loading standalone sensor data...</div>
        ) : !chartData.length ? (
          <div style={styles.emptyState}>
            {selectedSensor ? `No data available for ${formatSensorName(selectedSensor)}.` : 'Select a sensor above.'}
          </div>
        ) : (
          <>
            {/* Legend */}
            <div style={{ ...styles.controlContainer, marginBottom: '12px' }}>
              <div style={styles.legendRow}>
                {METRIC_OPTIONS.filter(m => visibleMetrics[m.key]).map(m => (
                  <div key={m.key} style={{ ...styles.legendItem, cursor: 'pointer' }} onClick={() => toggleMetric(m.key)}>
                    <div style={{ width: '14px', height: '3px', background: m.color, borderRadius: '2px', borderTop: m.dash ? `1px dashed ${m.color}` : 'none' }} />
                    <span style={{ fontWeight: 600 }}>{m.label}</span>
                  </div>
                ))}
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
              </div>
            </div>

            <div
              ref={scrollContainerRef}
              onScroll={handleChartScroll}
              style={allDaysMode ? { overflowX: 'auto', overflowY: 'hidden', width: '100%', position: 'relative' } : {}}
            >
            <div style={allDaysMode ? { width: `${scrollChartWidth}px`, minWidth: '100%' } : {}}>
            {/* Date + status callout */}
            {(() => {
              const displayDay = allDaysMode ? visibleDay : selectedDay;
              if (!displayDay) return null;
              return (
                <div style={{ position: 'sticky', left: 0, width: 'fit-content', marginBottom: '6px', display: 'flex', alignItems: 'center', gap: '8px', zIndex: 2 }}>
                  <div style={{
                    padding: '3px 12px', fontSize: '11px', fontWeight: 600, color: '#1B5E20',
                    background: 'rgba(243,248,243,0.9)', borderRadius: '6px', border: '1px solid rgba(176,205,174,0.5)',
                  }}>
                    {new Date(displayDay + 'T00:00:00').toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' })}
                  </div>
                  {statusByDay?.[displayDay] && (
                    <div title={statusByDay[displayDay].detail} style={{
                      padding: '3px 10px', fontSize: '11px', fontWeight: 600,
                      color: statusByDay[displayDay].color, background: `${statusByDay[displayDay].color}18`,
                      borderRadius: '999px', border: `1px solid ${statusByDay[displayDay].color}55`,
                    }}>
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
                margin={{ top: 10, right: 20, bottom: allDaysMode ? 20 : 5, left: 0 }}
                style={{ cursor: 'zoom-in' }}
                onClick={handleChartClick}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(203,230,200,0.4)" />
                {dayBoundaries.map((b) => (
                  <ReferenceLine key={`day-${b.day}`} x={b.idx} yAxisId="left" stroke="#F9A825" strokeWidth={2} strokeDasharray="8 4"
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
                    const row = chartData[idx];
                    if (!row) return null;
                    const label = allDaysMode ? (row.fullTs || '').substring(11, 16) : row.ts;
                    return (
                      <g>
                        <text x={x} y={y + 12} textAnchor="middle"
                          fontSize={9}
                          fill="#8A928A"
                        >{label}</text>
                      </g>
                    );
                  }}
                />
                <YAxis yAxisId="left" domain={yDomain} allowDataOverflow={true} ticks={yTicks} tick={{ fontSize: 10, fill: '#8A928A' }} tickLine={false} />
                <Tooltip content={<StandaloneTooltip downtimeBands={allDowntimeBands} alarmBands={visibleAlarmBands} />} wrapperStyle={{ zIndex: 1000, overflow: 'visible', pointerEvents: 'none' }} />

                {/* Downtime bands */}
                {allDowntimeBands.map((band, i) => (
                  <ReferenceArea key={`dt-${i}`} x1={band.start} x2={band.end} yAxisId="left"
                    fill="rgba(120,120,120,0.28)" fillOpacity={1} stroke="rgba(80,80,80,0.6)" strokeWidth={1.5} strokeDasharray="6 3" />
                ))}

                {/* Alarm bands */}
                {visibleAlarmBands.map((band, i) => {
                  const as = getAlarmStyle(band.severity);
                  return (
                    <ReferenceArea key={`alarm-${i}`} x1={band.start} x2={band.end} yAxisId="left"
                      fill={as.areaFill} fillOpacity={1} stroke={as.areaStroke} strokeWidth={1.5} />
                  );
                })}

                {/* Metric lines */}
                {METRIC_OPTIONS.map(m => visibleMetrics[m.key] ? (
                  <Line
                    key={m.key}
                    yAxisId="left"
                    type="monotone"
                    dataKey={m.key}
                    stroke={m.color}
                    strokeWidth={m.key === 'evidence' ? 1.5 : 1}
                    strokeDasharray={m.dash || undefined}
                    dot={false}
                    name={m.label}
                    animationDuration={600}
                    connectNulls={false}
                  />
                ) : null)}

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

export default StandaloneSensorChart;
