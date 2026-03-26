import React, { useState, useEffect, useMemo } from 'react';
import ReactDOM from 'react-dom';
import { motion, AnimatePresence } from 'framer-motion';
import {
  ResponsiveContainer, RadarChart, PolarGrid, PolarAngleAxis,
  PolarRadiusAxis, Radar, Legend, Tooltip,
  LineChart, Line, XAxis, YAxis, CartesianGrid, ReferenceArea,
} from 'recharts';
import GlassCard from './GlassCard';
import InfoTooltip from './InfoTooltip';
import { getBetaOverview, getBetaSubsystems, getBetaRadarFingerprints, getBetaSensorQualityWindow, getBetaAlerts } from '../utils/api';
import { systemColor, systemBgColor, formatSensorName } from '../utils/formatters';

const SENSOR_PALETTE = [
  '#1B5E20', '#0D47A1', '#E65100', '#7B1FA2', '#004D40',
  '#BF360C', '#1A237E', '#33691E', '#880E4F', '#01579B',
  '#F57F17', '#4A148C', '#006064', '#3E2723',
];

const METRIC_SUFFIXES = [
  { key: 'a', label: 'Engine A', color: '#E65100' },
  { key: 'b', label: 'Engine B', color: '#7B1FA2' },
];

const SQS_COLOR = '#1976D2';

const getAlarmStyle = (severity) => (
  severity === 'HIGH'
    ? { areaFill: 'rgba(239,83,80,0.22)', areaStroke: 'rgba(239,83,80,0.5)' }
    : severity === 'MEDIUM'
      ? { areaFill: 'rgba(255,167,38,0.20)', areaStroke: 'rgba(245,124,0,0.46)' }
      : { areaFill: 'rgba(120,144,156,0.16)', areaStroke: 'rgba(120,144,156,0.35)' }
);

const styles = {
  grid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(5, 1fr)',
    gap: '16px',
  },
  cardInner: {
    height: '160px',
    overflowY: 'auto',
    padding: '20px',
  },
  statLabel: {
    fontSize: '12px',
    fontWeight: 500,
    color: '#6B736B',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    marginBottom: '8px',
    display: 'flex',
    alignItems: 'center',
  },
  statValue: {
    fontSize: '28px',
    fontWeight: 600,
    color: '#1B5E20',
    letterSpacing: '-0.02em',
  },
  statSub: {
    fontSize: '12px',
    color: '#8A928A',
    marginTop: '4px',
  },
};

function SubsystemDetectedModal({ open, onClose, subsystems }) {
  if (!open) return null;
  return ReactDOM.createPortal(
    <AnimatePresence>
      {open && (
        <motion.div
          style={{
            position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
            background: 'rgba(0,0,0,0.35)', backdropFilter: 'blur(6px)', WebkitBackdropFilter: 'blur(6px)',
            zIndex: 9999, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '24px',
          }}
          initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
          onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
        >
          <motion.div
            style={{
              position: 'relative', width: '100%', maxWidth: '820px', maxHeight: '85vh', overflowY: 'auto',
              background: 'rgba(255,255,255,0.92)', backdropFilter: 'blur(28px)', WebkitBackdropFilter: 'blur(28px)',
              border: '1.5px solid rgba(203,230,200,0.5)', borderRadius: '20px', padding: '32px 36px 28px',
              boxShadow: '0 24px 80px rgba(0,0,0,0.15)',
            }}
            initial={{ opacity: 0, scale: 0.92, y: 30 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.92, y: 30 }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px', paddingBottom: '16px', borderBottom: '1px solid rgba(203,230,200,0.4)' }}>
              <div>
                <div style={{ fontSize: '16px', fontWeight: 700, color: '#1B5E20' }}>Detected Subsystems</div>
                <div style={{ fontSize: '12px', color: '#6B736B', marginTop: '2px' }}>
                  {subsystems.length} subsystem{subsystems.length !== 1 ? 's' : ''} identified via hierarchical clustering
                </div>
              </div>
              <motion.button
                style={{ width: '32px', height: '32px', borderRadius: '10px', background: 'rgba(27,94,32,0.08)', border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '16px', color: '#6B736B', fontWeight: 600 }}
                onClick={onClose} whileHover={{ background: 'rgba(27,94,32,0.15)' }} whileTap={{ scale: 0.9 }}
              >X</motion.button>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '14px' }}>
              {subsystems.map((sys, idx) => {
                const color = systemColor(sys.system_id, idx);
                const bg = systemBgColor(sys.system_id);
                return (
                  <motion.div
                    key={sys.system_id}
                    style={{
                      background: 'rgba(255,255,255,0.55)', backdropFilter: 'blur(20px)',
                      borderRadius: '18px', border: '1.5px solid rgba(203,230,200,0.45)',
                      padding: '20px 18px 18px', display: 'flex', flexDirection: 'column',
                    }}
                    initial={{ opacity: 0, y: 16 }} animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: idx * 0.06, duration: 0.4 }}
                  >
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
                      <div style={{ width: 10, height: 10, borderRadius: '50%', background: color, boxShadow: `0 0 8px ${color}55` }} />
                      <div style={{ fontSize: '18px', fontWeight: 700, color, textTransform: 'capitalize' }}>
                        {sys.system_id.replace(/_/g, ' ')}
                      </div>
                    </div>
                    <div style={{ fontSize: '12px', color: '#6B736B' }}>
                      {sys.sensor_count} sensor{sys.sensor_count !== 1 ? 's' : ''}
                      {sys.fusion_weight != null && ` | Weight ${(sys.fusion_weight * 100).toFixed(1)}%`}
                    </div>

                    {sys.sensors && sys.sensors.length > 0 && (
                      <div style={{ marginTop: '12px', paddingTop: '10px', borderTop: '1px solid rgba(203,230,200,0.3)' }}>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                          {sys.sensors.map((s) => (
                            <span key={s} style={{
                              display: 'inline-block', padding: '2px 8px', borderRadius: '7px',
                              fontSize: '10px', fontWeight: 500, background: bg, color: '#2D332D',
                              border: '1px solid rgba(0,0,0,0.05)',
                            }}>
                              {formatSensorName(s)}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}
                  </motion.div>
                );
              })}
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>,
    document.body
  );
}

/* ─── Radar Tooltip (shared) ─── */
const RadarTooltipContent = ({ active, payload }) => {
  if (!active || !payload || !payload.length) return null;
  const item = payload[0]?.payload;
  if (!item) return null;
  return (
    <div style={{
      background: 'rgba(255,255,255,0.92)', backdropFilter: 'blur(12px)',
      border: '1px solid rgba(203,230,200,0.5)', borderRadius: '10px',
      padding: '8px 12px', fontSize: '11px', boxShadow: '0 4px 12px rgba(0,0,0,0.08)',
    }}>
      <div style={{ fontWeight: 600, color: '#2c3e50', marginBottom: '2px' }}>{formatSensorName(item.sensor)}</div>
      <div style={{ color: '#e74c3c' }}>Fault fingerprint: {item.fault.toFixed(4)}</div>
      <div style={{ color: '#1B5E20' }}>Normal baseline: {item.baseline.toFixed(4)}</div>
      {item.sqs != null && <div style={{ color: '#6B736B' }}>SQS: {item.sqs.toFixed(4)}</div>}
      {item.trust && <div style={{ color: '#6B736B' }}>Trust: {item.trust}</div>}
    </div>
  );
};

/* ─── Peak Alarm Detail Modal (from overview card) ─── */
function PeakAlarmDetailModal({ fingerprint, onClose }) {
  if (!fingerprint) return null;
  const fp = fingerprint;
  const baselineVal = 0.5;

  /* ── Local state for chart data ── */
  const [chartLoading, setChartLoading] = useState(true);
  const [qualityData, setQualityData] = useState(null);
  const [alerts, setAlerts] = useState([]);
  const [modalViewMode, setModalViewMode] = useState('subsystem');
  const [modalMetric, setModalMetric] = useState('a');
  const [visibleSensors, setVisibleSensors] = useState({});

  /* Fetch chart data independently -- does NOT touch SensorQualityGrid */
  useEffect(() => {
    let cancelled = false;
    setChartLoading(true);
    // Compute padded window around the event to limit data transfer
    const evStart = fp.event_start ? new Date(fp.event_start) : null;
    const evEnd = fp.event_end ? new Date(fp.event_end) : null;
    let winStart = null, winEnd = null;
    if (evStart && evEnd) {
      const dur = evEnd - evStart;
      const pad = Math.max(dur * 1.5, 60000); // 1.5x duration or 1min minimum
      winStart = new Date(evStart - pad).toISOString();
      winEnd = new Date(evEnd.getTime() + pad).toISOString();
    }
    Promise.all([
      getBetaSensorQualityWindow(fp.system_id, winStart, winEnd, 1),
      getBetaAlerts({ alarm_view: 'subsystem' }).catch(() => ({ data: { alerts: [] } })),
    ]).then(([qRes, aRes]) => {
      if (cancelled) return;
      setQualityData(qRes.data);
      setAlerts(aRes.data.alerts || []);
      const vis = {};
      (qRes.data.sensors || []).forEach((s) => { vis[s] = true; });
      setVisibleSensors(vis);
    }).catch(() => {}).finally(() => { if (!cancelled) setChartLoading(false); });
    return () => { cancelled = true; };
  }, [fp.system_id]);

  const sensors = qualityData?.sensors || [];

  /* Build subsystem-level chart rows */
  const subsystemChartData = useMemo(() => {
    if (!qualityData?.subsystem_timeseries?.length) return [];
    return qualityData.subsystem_timeseries.map((row, i) => ({
      ...row, _idx: i,
      fullTs: row.ts ? String(row.ts) : '',
      ts: row.ts ? String(row.ts).substring(11, 16) : '',
    }));
  }, [qualityData]);

  /* Build sensor-level chart rows */
  const sensorChartData = useMemo(() => {
    if (!qualityData?.timeseries?.length) return [];
    const allSensors = qualityData.sensors || [];
    return qualityData.timeseries.map((row, i) => {
      const sqsVals = allSensors.map((s) => {
        const v = row[`${s}__sqs`];
        return (v != null && isFinite(v)) ? v : 0;
      });
      const avgSqsPct = sqsVals.length ? (sqsVals.reduce((a, b) => a + b, 0) / sqsVals.length) * 100 : null;
      return {
        ...row, _idx: i, _avgSqsPct: avgSqsPct,
        fullTs: row.ts ? String(row.ts) : '',
        ts: row.ts ? String(row.ts).substring(11, 16) : '',
      };
    });
  }, [qualityData]);

  const chartData = modalViewMode === 'subsystem' ? subsystemChartData : sensorChartData;

  /* Extract event window slice */
  const eventSlice = useMemo(() => {
    if (!chartData.length || !fp.event_start || !fp.event_end) return [];
    const evStart = new Date(fp.event_start).getTime();
    const evEnd = new Date(fp.event_end).getTime();
    const pad = (evEnd - evStart) * 1.5 || 30 * 60 * 1000;
    return chartData.filter((row) => {
      if (!row.fullTs) return false;
      const t = new Date(row.fullTs).getTime();
      return t >= evStart - pad && t <= evEnd + pad;
    });
  }, [chartData, fp.event_start, fp.event_end]);

  /* Alarm bands for event slice */
  const eventAlarmBands = useMemo(() => {
    if (!alerts.length || !eventSlice.length) return [];
    const sysAlerts = alerts.filter((a) => a.system_id === fp.system_id);
    const bands = [];
    for (const a of sysAlerts) {
      const aStart = new Date(a.start_ts || a.event_start).getTime();
      const aEnd = new Date(a.end_ts || a.event_end).getTime();
      let startIdx = null, endIdx = null;
      for (const row of eventSlice) {
        const t = new Date(row.fullTs).getTime();
        if (t >= aStart && startIdx === null) startIdx = row._idx;
        if (t <= aEnd) endIdx = row._idx;
      }
      if (startIdx != null && endIdx != null) {
        bands.push({ start: startIdx, end: endIdx, severity: a.severity || 'MEDIUM' });
      }
    }
    return bands;
  }, [alerts, eventSlice, fp.system_id]);

  /* Y domain for event chart */
  const eventYDomain = useMemo(() => {
    if (!eventSlice.length) return ['auto', 'auto'];
    let min = Infinity, max = -Infinity;
    if (modalViewMode === 'sensor' && sensors.length) {
      for (const row of eventSlice) {
        for (const s of sensors) {
          if (!visibleSensors[s]) continue;
          const v = row[`${s}__${modalMetric}`];
          if (v != null && isFinite(v)) { if (v < min) min = v; if (v > max) max = v; }
        }
      }
    } else {
      for (const row of eventSlice) {
        for (const key of ['system_score', 'adaptive_threshold']) {
          const v = row[key];
          if (v != null && isFinite(v)) { if (v < min) min = v; if (v > max) max = v; }
        }
      }
    }
    if (!isFinite(min) || !isFinite(max)) return ['auto', 'auto'];
    const range = max - min;
    const p = range > 0 ? range * 0.1 : 0.1;
    return [Math.max(0, Math.floor((min - p) * 100) / 100), Math.ceil((max + p) * 100) / 100];
  }, [eventSlice, modalViewMode, sensors, visibleSensors, modalMetric]);

  const radarData = fp.sensors.map((s) => ({
    sensor: s.sensor,
    sensorLabel: formatSensorName(s.sensor),
    fault: s.value,
    baseline: baselineVal,
    sqs: fp.sensor_meta?.[s.sensor]?.sqs ?? null,
    trust: fp.sensor_meta?.[s.sensor]?.trust ?? null,
  }));

  const sortedSensors = radarData.slice().sort((a, b) => b.fault - a.fault);
  const topSensor = sortedSensors[0] || null;
  const maxVal = Math.max(...radarData.map((d) => d.fault), baselineVal, 0.5);

  const subtitle = fp.has_alarm && fp.event_start
    ? `Peak risk=${fp.peak_risk.toFixed(3)}  |  Event: ${fp.event_start.substring(0, 16)} to ${fp.event_end.substring(0, 16)}`
    : 'No alarms detected -- showing latest AE contributions';

  return ReactDOM.createPortal(
    <AnimatePresence>
      <motion.div
        initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
        transition={{ duration: 0.25 }}
        onClick={onClose}
        style={{
          position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
          background: 'rgba(26, 31, 26, 0.4)', backdropFilter: 'blur(16px)',
          WebkitBackdropFilter: 'blur(16px)', zIndex: 1100,
          display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px',
        }}
      >
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 12 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 12 }}
          transition={{ duration: 0.3, ease: [0.4, 0, 0.2, 1] }}
          onClick={(e) => e.stopPropagation()}
          style={{
            background: 'rgba(255, 255, 255, 0.88)', backdropFilter: 'blur(32px)',
            WebkitBackdropFilter: 'blur(32px)', borderRadius: '24px',
            border: '1px solid rgba(203, 230, 200, 0.5)',
            boxShadow: '0 32px 100px rgba(27,94,32,0.2), 0 8px 32px rgba(27,94,32,0.08)',
            width: '100%', maxWidth: '760px', maxHeight: '90vh', overflow: 'auto', padding: '32px',
          }}
        >
          {/* Header */}
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '16px' }}>
            <div>
              <div style={{ fontSize: '20px', fontWeight: 600, color: '#1B5E20', letterSpacing: '-0.02em' }}>
                Fault Fingerprint -- {fp.system_id}
              </div>
              <div style={{ fontSize: '11px', color: '#8A928A', marginTop: '4px' }}>{subtitle}</div>
            </div>
            <div
              onClick={onClose}
              style={{
                width: '32px', height: '32px', borderRadius: '50%', display: 'flex',
                alignItems: 'center', justifyContent: 'center', cursor: 'pointer',
                background: 'rgba(27,94,32,0.06)', border: '1px solid rgba(27,94,32,0.12)',
                fontSize: '16px', color: '#6B736B', transition: 'all 0.2s',
              }}
            >x</div>
          </div>

          {/* Badges */}
          <div style={{ display: 'flex', gap: '8px', marginBottom: '16px', flexWrap: 'wrap' }}>
            {fp.has_alarm && (
              <span style={{
                fontSize: '10px', fontWeight: 700, padding: '3px 10px', borderRadius: '8px',
                background: 'rgba(239,83,80,0.10)', border: '1px solid rgba(239,83,80,0.3)',
                color: '#D32F2F', letterSpacing: '0.04em',
              }}>PEAK RISK: {fp.peak_risk.toFixed(4)}</span>
            )}
            <span style={{
              fontSize: '10px', fontWeight: 600, padding: '3px 10px', borderRadius: '8px',
              background: 'rgba(27,94,32,0.06)', border: '1px solid rgba(27,94,32,0.15)', color: '#1B5E20',
            }}>{fp.sensor_count} sensors</span>
            {fp.alarm_count > 0 && (
              <span style={{
                fontSize: '10px', fontWeight: 600, padding: '3px 10px', borderRadius: '8px',
                background: 'rgba(230,81,0,0.08)', border: '1px solid rgba(230,81,0,0.2)', color: '#E65100',
              }}>{fp.alarm_count} alarm event{fp.alarm_count > 1 ? 's' : ''}</span>
            )}
          </div>

          {/* Event Window Chart */}
          {chartLoading ? (
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '120px', color: '#8A928A', fontSize: '12px' }}>
              Loading event chart...
            </div>
          ) : eventSlice.length > 0 && (
            <div style={{ marginBottom: '16px' }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '6px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <div style={{ fontSize: '10px', fontWeight: 700, color: '#8A928A', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                    Event Window
                  </div>
                  {fp.system_id !== 'ISOLATED' && (
                    <div style={{ display: 'flex', gap: '3px' }}>
                      {['subsystem', 'sensor'].map((v) => (
                        <div key={v} onClick={() => setModalViewMode(v)} style={{
                          padding: '2px 10px', borderRadius: '10px', fontSize: '9px', fontWeight: modalViewMode === v ? 600 : 400,
                          cursor: 'pointer', border: modalViewMode === v ? '1.5px solid #1B5E20' : '1px solid rgba(203,230,200,0.4)',
                          background: modalViewMode === v ? 'rgba(27,94,32,0.08)' : 'transparent',
                          color: modalViewMode === v ? '#1B5E20' : '#8A928A',
                        }}>
                          {v === 'subsystem' ? 'System Score' : 'Sensor Breakdown'}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
                {modalViewMode === 'sensor' && (
                  <div style={{ display: 'flex', gap: '4px' }}>
                    {METRIC_SUFFIXES.map(m => (
                      <div key={m.key} style={{
                        padding: '2px 10px', borderRadius: '10px', fontSize: '10px', fontWeight: modalMetric === m.key ? 600 : 400,
                        cursor: 'pointer', border: modalMetric === m.key ? `1.5px solid ${m.color}` : '1px solid rgba(203,230,200,0.4)',
                        background: modalMetric === m.key ? `${m.color}14` : 'transparent',
                        color: modalMetric === m.key ? m.color : '#8A928A',
                      }} onClick={() => setModalMetric(m.key)}>
                        {m.label}
                      </div>
                    ))}
                  </div>
                )}
              </div>
              <ResponsiveContainer width="100%" height={180}>
                <LineChart data={eventSlice} margin={{ top: 5, right: modalViewMode === 'sensor' ? 50 : 12, bottom: 5, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(203,230,200,0.4)" />
                  <XAxis
                    dataKey="_idx" type="number" domain={['dataMin', 'dataMax']}
                    tick={{ fontSize: 9, fill: '#8A928A' }} tickLine={false}
                    tickFormatter={(idx) => {
                      const row = eventSlice.find((r) => r._idx === idx);
                      return row?.ts || '';
                    }}
                  />
                  <YAxis yAxisId="left" domain={eventYDomain} tick={{ fontSize: 9, fill: '#8A928A' }} tickLine={false} />
                  {modalViewMode === 'sensor' && (
                    <YAxis yAxisId="right" orientation="right" domain={[0, 100]} tick={{ fontSize: 8, fill: SQS_COLOR }} tickLine={false} tickFormatter={(v) => `${v}%`} />
                  )}
                  <Tooltip
                    content={({ active, payload }) => {
                      if (!active || !payload?.length) return null;
                      const row = payload[0]?.payload;
                      return (
                        <div style={{
                          background: 'rgba(255,255,255,0.94)', backdropFilter: 'blur(12px)',
                          border: '1px solid rgba(203,230,200,0.5)', borderRadius: '10px',
                          padding: '8px 12px', fontSize: '11px', boxShadow: '0 4px 12px rgba(0,0,0,0.08)',
                        }}>
                          <div style={{ fontWeight: 600, color: '#1B5E20', marginBottom: '4px' }}>
                            {row?.fullTs ? String(row.fullTs).substring(0, 19) : ''}
                          </div>
                          {payload.map((p, i) => (
                            <div key={i} style={{ display: 'flex', justifyContent: 'space-between', gap: '12px' }}>
                              <span style={{ color: p.color }}>{p.name}</span>
                              <span style={{ fontWeight: 500 }}>{p.value != null ? Number(p.value).toFixed(4) : '--'}</span>
                            </div>
                          ))}
                        </div>
                      );
                    }}
                  />
                  {eventAlarmBands.map((band, i) => {
                    const alarmStyle = getAlarmStyle(band.severity);
                    return (
                      <ReferenceArea
                        key={`ev-alarm-${i}`}
                        yAxisId="left"
                        x1={band.start} x2={band.end}
                        fill={alarmStyle.areaFill}
                        fillOpacity={1}
                        stroke={alarmStyle.areaStroke}
                        strokeWidth={1.5}
                      />
                    );
                  })}
                  {modalViewMode === 'sensor' ? (
                    <>
                      <Line yAxisId="right" type="monotone" dataKey="_avgSqsPct" stroke={SQS_COLOR} strokeWidth={2} dot={false} name="Avg SQS %" connectNulls />
                      {sensors.map((s, i) =>
                        visibleSensors[s] ? (
                          <Line
                            key={s}
                            yAxisId="left"
                            type="monotone"
                            dataKey={`${s}__${modalMetric}`}
                            stroke={SENSOR_PALETTE[i % SENSOR_PALETTE.length]}
                            strokeWidth={1.5}
                            dot={false}
                            name={formatSensorName(s)}
                            connectNulls={false}
                          />
                        ) : null
                      )}
                    </>
                  ) : (
                    <>
                      <Line yAxisId="left" type="monotone" dataKey="system_score" stroke="#1B5E20" strokeWidth={2} dot={false} name="System Score" />
                      <Line yAxisId="left" type="monotone" dataKey="adaptive_threshold" stroke="#E65100" strokeWidth={1.5} strokeDasharray="6 3" dot={false} name="Adaptive Threshold" />
                    </>
                  )}
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Radar Chart */}
          <ResponsiveContainer width="100%" height={380}>
            <RadarChart data={radarData} cx="50%" cy="50%" outerRadius="70%">
              <PolarGrid stroke="rgba(27,94,32,0.12)" />
              <PolarAngleAxis dataKey="sensorLabel" tick={{ fontSize: 9, fill: '#6B736B' }} />
              <PolarRadiusAxis
                angle={90} domain={[0, Math.ceil(maxVal * 10) / 10]}
                tick={{ fontSize: 8, fill: '#8A928A' }} tickCount={5}
              />
              <Radar name="Normal baseline (0.5s)" dataKey="baseline" stroke="#1B5E20" fill="#1B5E20" fillOpacity={0.06} strokeWidth={1.5} strokeDasharray="5 3" dot={false} />
              <Radar name="Fault fingerprint" dataKey="fault" stroke="#e74c3c" fill="#e74c3c" fillOpacity={0.15} strokeWidth={2} dot={{ r: 3, fill: '#e74c3c', strokeWidth: 0 }} />
              <Legend wrapperStyle={{ fontSize: '11px', paddingTop: '8px' }} iconType="line" />
              <Tooltip content={<RadarTooltipContent />} />
            </RadarChart>
          </ResponsiveContainer>

          {/* Top contributor */}
          {topSensor && (
            <div style={{
              display: 'flex', alignItems: 'center', gap: '10px',
              padding: '10px 14px', borderRadius: '10px', marginTop: '8px',
              background: 'rgba(239,83,80,0.04)', border: '1px solid rgba(239,83,80,0.15)',
            }}>
              <div>
                <div style={{ fontSize: '9.5px', fontWeight: 600, color: '#8A928A', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Top Contributor</div>
                <div style={{ fontSize: '13px', fontWeight: 700, color: '#2c3e50' }}>{formatSensorName(topSensor.sensor)}</div>
                <div style={{ fontSize: '10px', color: '#8A928A' }}>
                  Value: {topSensor.fault.toFixed(4)}
                  {topSensor.sqs != null && ` | SQS: ${topSensor.sqs.toFixed(2)}`}
                  {topSensor.trust && ` | ${topSensor.trust}`}
                </div>
              </div>
            </div>
          )}

          {/* Sensor table */}
          <div style={{ marginTop: '14px' }}>
            <div style={{ fontSize: '10px', fontWeight: 700, color: '#8A928A', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: '6px' }}>
              Sensor Detail ({sortedSensors.length})
            </div>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px' }}>
              <thead>
                <tr>
                  <th style={{ padding: '6px 8px', textAlign: 'left', fontWeight: 600, color: '#8A928A', fontSize: '9.5px', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: '1px solid rgba(203,230,200,0.3)' }}>Sensor</th>
                  <th style={{ padding: '6px 8px', textAlign: 'right', fontWeight: 600, color: '#8A928A', fontSize: '9.5px', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: '1px solid rgba(203,230,200,0.3)' }}>Value</th>
                  <th style={{ padding: '6px 8px', textAlign: 'right', fontWeight: 600, color: '#8A928A', fontSize: '9.5px', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: '1px solid rgba(203,230,200,0.3)' }}>SQS</th>
                  <th style={{ padding: '6px 8px', textAlign: 'center', fontWeight: 600, color: '#8A928A', fontSize: '9.5px', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: '1px solid rgba(203,230,200,0.3)' }}>Trust</th>
                </tr>
              </thead>
              <tbody>
                {sortedSensors.map((s) => (
                  <tr key={s.sensor}>
                    <td style={{ padding: '5px 8px', borderBottom: '1px solid rgba(203,230,200,0.15)', color: '#2c3e50' }}>{formatSensorName(s.sensor)}</td>
                    <td style={{ padding: '5px 8px', borderBottom: '1px solid rgba(203,230,200,0.15)', color: '#2c3e50', textAlign: 'right', fontWeight: 600 }}>{s.fault.toFixed(4)}</td>
                    <td style={{ padding: '5px 8px', borderBottom: '1px solid rgba(203,230,200,0.15)', color: '#2c3e50', textAlign: 'right' }}>{s.sqs != null ? s.sqs.toFixed(2) : '--'}</td>
                    <td style={{ padding: '5px 8px', borderBottom: '1px solid rgba(203,230,200,0.15)', color: '#2c3e50', textAlign: 'center' }}>
                      {s.trust ? (
                        <span style={{
                          fontSize: '9px', fontWeight: 600, padding: '2px 6px', borderRadius: '8px',
                          display: 'inline-block',
                          backgroundColor: s.trust === 'Reliable' ? 'rgba(27,94,32,0.08)' : 'rgba(231,76,60,0.08)',
                          color: s.trust === 'Reliable' ? '#1B5E20' : '#c0392b',
                        }}>{s.trust}</span>
                      ) : '--'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </motion.div>
      </motion.div>
    </AnimatePresence>,
    document.body
  );
}

function BetaOverviewCards() {
  const [overview, setOverview] = useState(null);
  const [subsystems, setSubsystems] = useState([]);
  const [showSystems, setShowSystems] = useState(false);
  const [fingerprints, setFingerprints] = useState([]);
  const [selectedFp, setSelectedFp] = useState(null);

  useEffect(() => {
    // Fetch overview and subsystems first (lightweight), then fingerprints (heavy)
    // so the first 4 cards render quickly without being blocked by Flask's single thread.
    Promise.all([
      getBetaOverview().then(res => setOverview(res.data)).catch(() => {}),
      getBetaSubsystems().then(res => setSubsystems(res.data.subsystems || [])).catch(() => {}),
    ]).then(() => {
      getBetaRadarFingerprints().then(res => setFingerprints(res.data.fingerprints || [])).catch(() => {});
    });
  }, []);

  const nonIsolated = subsystems.filter(s => s.system_id !== 'ISOLATED');

  return (
    <div>
      <div style={styles.grid}>
        {/* System Selected */}
        <GlassCard delay={0.05} intensity="strong" padding="0">
          <div style={styles.cardInner}>
            <div style={styles.statLabel}>
              System Selected
              <InfoTooltip text="The industrial system currently being monitored. All sensors belong to this system." />
            </div>
            <div style={styles.statValue}>{overview?.system_name || 'Shredder'}</div>
            <div style={styles.statSub}>
              {overview?.data_range?.start && (
                <span>{String(overview.data_range.start).substring(0, 10)} to {String(overview.data_range.end).substring(0, 10)}</span>
              )}
            </div>
          </div>
        </GlassCard>

        {/* Sensors Found */}
        <GlassCard delay={0.1} intensity="strong" padding="0">
          <div style={styles.cardInner}>
            <div style={styles.statLabel}>
              Sensors Found
              <InfoTooltip text="Total unique sensor channels discovered in the dataset after pivot and cleaning." />
            </div>
            <div style={styles.statValue}>{overview?.total_sensors ?? '--'}</div>
            <div style={styles.statSub}>active sensor channels</div>
          </div>
        </GlassCard>

        {/* Downtime Detected */}
        <GlassCard delay={0.15} intensity="strong" padding="0">
          <div style={styles.cardInner}>
            <div style={styles.statLabel}>
              Downtime Detected
              <InfoTooltip text="Downtime is detected when electrical signals (kW, current, voltage, frequency) fall below auto-computed thresholds. All risk scores are gated to 0 during downtime." />
            </div>
            <div style={styles.statValue}>
              {overview?.downtime_pct !== undefined ? `${overview.downtime_pct}%` : '--'}
            </div>
            <div style={styles.statSub}>
              {overview ? `${overview.downtime_minutes?.toLocaleString()} / ${overview.total_minutes?.toLocaleString()} min` : '--'}
            </div>
            <div style={{
              marginTop: '8px', height: '6px', borderRadius: '3px', background: '#E8ECE8', overflow: 'hidden',
            }}>
              <div style={{
                height: '100%', borderRadius: '3px',
                width: `${overview?.running_pct || 0}%`,
                background: 'linear-gradient(90deg, #81C784, #4CAF50)',
                transition: 'width 1s ease',
              }} />
            </div>
            <div style={{ fontSize: '11px', color: '#4CAF50', marginTop: '4px', fontWeight: 500 }}>
              {overview?.running_pct ?? '--'}% running
            </div>
          </div>
        </GlassCard>

        {/* Subsystems Detected */}
        <GlassCard delay={0.2} intensity="strong" padding="0">
          <div style={styles.cardInner}>
          <div style={styles.statLabel}>
            Subsystems Detected
            <InfoTooltip text="Subsystems discovered via hierarchical correlation clustering. Each groups sensors with correlated behavior. Isolated sensors don't cluster with any group." />
          </div>
          <div style={styles.statValue}>{nonIsolated.length || '--'}</div>
          <div style={styles.statSub}>
            {subsystems.find(s => s.system_id === 'ISOLATED')
              ? `+ ${subsystems.find(s => s.system_id === 'ISOLATED').sensor_count} isolated`
              : ''}
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px', marginTop: '8px' }}>
            {nonIsolated.map((sys, idx) => (
              <span key={sys.system_id} style={{
                display: 'inline-block', padding: '3px 10px', borderRadius: '12px',
                fontSize: '11px', fontWeight: 600,
                background: systemBgColor(sys.system_id),
                color: systemColor(sys.system_id, idx),
              }}>
                {sys.system_id.replace('_', ' ')} ({sys.sensor_count})
              </span>
            ))}
          </div>
          <motion.button
            onClick={() => setShowSystems(true)}
            style={{
              marginTop: '8px', display: 'inline-flex', alignItems: 'center', gap: '6px',
              padding: '5px 14px', background: 'rgba(27,94,32,0.08)',
              border: '1.5px solid rgba(27,94,32,0.2)', borderRadius: '16px',
              fontSize: '11px', fontWeight: 600, color: '#1B5E20', cursor: 'pointer',
            }}
            whileHover={{ background: 'rgba(230,244,234,0.9)', borderColor: 'rgba(27,94,32,0.45)' }}
            whileTap={{ scale: 0.97 }}
          >
            Check subsystems
          </motion.button>
          </div>
        </GlassCard>

        {/* Peak Alarms */}
        <GlassCard delay={0.25} intensity="strong" padding="0">
          <div style={styles.cardInner}>
            <div style={styles.statLabel}>
              Peak Alarms
              <InfoTooltip text="Summary of peak alarm events detected per subsystem. Each entry shows the highest-risk event window identified by the autoencoder fault fingerprint analysis." />
            </div>
            {(() => {
              const alarmFps = fingerprints.filter(f => f.has_alarm && f.event_start);
              if (!alarmFps.length) {
                return (
                  <>
                    <div style={styles.statValue}>0</div>
                    <div style={styles.statSub}>No peak alarm events</div>
                  </>
                );
              }
              return (
                <>
                  <div style={styles.statValue}>{alarmFps.length}</div>
                  <div style={styles.statSub}>across {alarmFps.length} subsystem{alarmFps.length > 1 ? 's' : ''}</div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '5px', marginTop: '8px' }}>
                    {alarmFps.map((fp, idx) => {
                      const color = systemColor(fp.system_id, idx);
                      const topSensor = fp.sensors?.slice().sort((a, b) => b.value - a.value)[0];
                      return (
                        <div
                          key={fp.system_id}
                          onClick={() => setSelectedFp(fp)}
                          style={{
                            display: 'flex', alignItems: 'center', gap: '8px',
                            padding: '5px 8px', borderRadius: '8px', cursor: 'pointer',
                            background: 'rgba(239,83,80,0.04)', border: '1px solid rgba(239,83,80,0.12)',
                            transition: 'all 0.15s',
                          }}
                          onMouseEnter={(e) => { e.currentTarget.style.background = 'rgba(239,83,80,0.08)'; }}
                          onMouseLeave={(e) => { e.currentTarget.style.background = 'rgba(239,83,80,0.04)'; }}
                        >
                          <span style={{ width: '6px', height: '6px', borderRadius: '50%', background: color, flexShrink: 0 }} />
                          <div style={{ flex: 1, minWidth: 0 }}>
                            <div style={{ fontSize: '11px', fontWeight: 600, color: '#2c3e50', display: 'flex', justifyContent: 'space-between' }}>
                              <span>{fp.system_id}</span>
                              <span style={{ fontSize: '10px', fontWeight: 700, color: '#D32F2F' }}>{fp.peak_risk.toFixed(3)}</span>
                            </div>
                            <div style={{ fontSize: '9px', color: '#8A928A', marginTop: '1px' }}>
                              {fp.event_start.substring(0, 10)} {fp.event_start.substring(11, 16)}--{fp.event_end.substring(11, 16)}
                              {topSensor && <span> | {formatSensorName(topSensor.sensor)}</span>}
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </>
              );
            })()}
          </div>
        </GlassCard>
      </div>

      <SubsystemDetectedModal open={showSystems} onClose={() => setShowSystems(false)} subsystems={subsystems} />
      {selectedFp && (
        <PeakAlarmDetailModal fingerprint={selectedFp} onClose={() => setSelectedFp(null)} />
      )}
    </div>
  );
}

export default BetaOverviewCards;
