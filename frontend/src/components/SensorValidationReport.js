import React, { useState, useEffect, useRef, useCallback } from 'react';
import ReactDOM from 'react-dom';
import { motion, AnimatePresence } from 'framer-motion';
import GlassCard from './GlassCard';
import InfoTooltip from './InfoTooltip';
import { getBetaSensorValidationReport } from '../utils/api';
import { formatSensorName } from '../utils/formatters';

const pct = (v) => `${(v * 100).toFixed(1)}%`;

function parseReasons(raw) {
  if (!raw) return [];
  return raw.split(' | ').map(chunk => {
    if (chunk.startsWith('HIGH_MISSINGNESS')) {
      const m = chunk.match(/missing_ratio=([\d.]+)\s*>\s*([\d.]+)/);
      return {
        label: 'Data Coverage',
        desc: 'Too much missing data',
        actual: m ? parseFloat(m[1]) : null,
        threshold: m ? parseFloat(m[2]) : null,
        displayActual: m ? pct(1 - parseFloat(m[1])) : '--',
        displayThreshold: m ? `> ${pct(1 - parseFloat(m[2]))}` : '--',
        gaugeValue: m ? 1 - parseFloat(m[1]) : 0,
        gaugeMax: m ? 1 - parseFloat(m[2]) : 0.6,
        color: '#EF5350',
      };
    }
    if (chunk.startsWith('CONSTANT_SIGNAL')) {
      const m = chunk.match(/std=([\d.e+-]+)\s*<\s*([\d.e+-]+)/);
      return {
        label: 'Signal Variance',
        desc: 'No meaningful change detected',
        actual: m ? parseFloat(m[1]) : null,
        displayActual: m ? parseFloat(m[1]).toExponential(1) : '--',
        displayThreshold: m ? `> ${parseFloat(m[2]).toExponential(0)}` : '--',
        gaugeValue: 0,
        gaugeMax: 1,
        color: '#EF5350',
      };
    }
    if (chunk.startsWith('NEAR_CONSTANT')) {
      const m = chunk.match(/unique_ratio=([\d.]+)\s*<\s*([\d.]+)/);
      return {
        label: 'Signal Diversity',
        desc: 'Almost all readings identical',
        actual: m ? parseFloat(m[1]) : null,
        displayActual: m ? pct(parseFloat(m[1])) : '--',
        displayThreshold: m ? `> ${pct(parseFloat(m[2]))}` : '--',
        gaugeValue: m ? parseFloat(m[1]) : 0,
        gaugeMax: m ? parseFloat(m[2]) : 0.01,
        color: '#EF5350',
      };
    }
    return { label: chunk, desc: '', displayActual: '--', displayThreshold: '--', gaugeValue: 0, gaugeMax: 1, color: '#EF5350' };
  });
}

function MiniGauge({ value, max, color, size = 48 }) {
  const normalized = max > 0 ? Math.min(Math.max(value / max, 0), 1) : 0;
  const cx = size / 2;
  const cy = size / 2;
  const r = size / 2 - 8;
  const arcPath = (startAngle, endAngle) => {
    const s = ((startAngle - 90) * Math.PI) / 180;
    const e = ((endAngle - 90) * Math.PI) / 180;
    const x1 = cx + r * Math.cos(s);
    const y1 = cy + r * Math.sin(s);
    const x2 = cx + r * Math.cos(e);
    const y2 = cy + r * Math.sin(e);
    const largeArc = endAngle - startAngle > 180 ? 1 : 0;
    return `M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2}`;
  };
  const needleAngle = -120 + normalized * 240;
  const needleRad = ((needleAngle - 90) * Math.PI) / 180;
  const needleEnd = { x: cx + (r - 4) * Math.cos(needleRad), y: cy + (r - 4) * Math.sin(needleRad) };
  const ticks = [];
  for (let i = 0; i <= 10; i++) {
    const tickAngle = -120 + i * 24;
    const rad = ((tickAngle - 90) * Math.PI) / 180;
    const outerR = r + 2;
    const innerR = r - (i % 5 === 0 ? 3 : 1.5);
    ticks.push(
      <line key={i}
        x1={cx + innerR * Math.cos(rad)} y1={cy + innerR * Math.sin(rad)}
        x2={cx + outerR * Math.cos(rad)} y2={cy + outerR * Math.sin(rad)}
        stroke={i % 5 === 0 ? '#4A524A' : '#B0B8B0'}
        strokeWidth={i % 5 === 0 ? 1 : 0.5} strokeLinecap="round"
      />
    );
  }
  return (
    <svg width={size} height={size * 0.75} viewBox={`0 0 ${size} ${size * 0.85}`} style={{ flexShrink: 0 }}>
      <path d={arcPath(-120, 120)} fill="none" stroke="#E8ECE8" strokeWidth="3.5" strokeLinecap="round" />
      {normalized > 0.005 && (
        <path d={arcPath(-120, -120 + normalized * 240)} fill="none" stroke={color} strokeWidth="3.5" strokeLinecap="round" opacity="0.8" />
      )}
      {ticks}
      <line x1={cx} y1={cy} x2={needleEnd.x} y2={needleEnd.y} stroke="#1A1F1A" strokeWidth="1.2" strokeLinecap="round" />
      <circle cx={cx} cy={cy} r="2" fill="#1A1F1A" />
    </svg>
  );
}

function FailedInfoButton({ sensor }) {
  const [visible, setVisible] = useState(false);
  const [pos, setPos] = useState({ top: 0, left: 0 });
  const triggerRef = useRef(null);
  const tooltipRef = useRef(null);
  const reasons = parseReasons(sensor.removal_reasons);

  const reposition = useCallback(() => {
    if (!triggerRef.current) return;
    const rect = triggerRef.current.getBoundingClientRect();
    const pad = 10;
    let top = rect.bottom + pad;
    let left = rect.left + rect.width / 2 - 150;
    if (left + 300 > window.innerWidth - pad) left = window.innerWidth - 300 - pad;
    if (left < pad) left = pad;
    if (top + 200 > window.innerHeight - pad) top = rect.top - 200 - pad;
    setPos({ top, left });
  }, []);

  useEffect(() => {
    if (!visible || !tooltipRef.current || !triggerRef.current) return;
    const rect = triggerRef.current.getBoundingClientRect();
    const tr = tooltipRef.current.getBoundingClientRect();
    const pad = 10;
    let top = rect.bottom + pad;
    let left = rect.left + rect.width / 2 - tr.width / 2;
    if (left + tr.width > window.innerWidth - pad) left = window.innerWidth - tr.width - pad;
    if (left < pad) left = pad;
    if (top + tr.height > window.innerHeight - pad) top = rect.top - tr.height - pad;
    if (top < pad) top = pad;
    setPos({ top, left });
  }, [visible]);

  return (
    <>
      <span
        ref={triggerRef}
        onMouseEnter={() => { reposition(); setVisible(true); }}
        onMouseLeave={() => setVisible(false)}
        className="tooltip-trigger"
        style={{ marginLeft: '2px', cursor: 'default' }}
      >
        i
      </span>
      {visible && ReactDOM.createPortal(
        <div
          ref={tooltipRef}
          style={{
            position: 'fixed', top: pos.top, left: pos.left, zIndex: 99999,
            background: 'rgba(255,255,255,0.97)', backdropFilter: 'blur(24px)', WebkitBackdropFilter: 'blur(24px)',
            border: '1px solid rgba(203,230,200,0.6)', borderRadius: '14px',
            padding: '14px 16px', boxShadow: '0 12px 40px rgba(27,94,32,0.15), 0 4px 12px rgba(27,94,32,0.08)',
            minWidth: '240px', maxWidth: '300px', pointerEvents: 'none',
          }}
        >
          <div style={{ fontSize: '11.5px', fontWeight: 700, color: '#C62828', marginBottom: '2px' }}>
            {formatSensorName(sensor.sensor)}
          </div>
          <div style={{ fontSize: '10px', color: '#8A928A', marginBottom: '6px' }}>
            Coverage {pct(1 - sensor.missing_ratio)} -- Diversity {pct(sensor.unique_ratio)}
          </div>
          <div style={{ borderTop: '1px solid rgba(0,0,0,0.05)', paddingTop: '8px' }}>
            {reasons.map((r, i) => (
              <div key={i} style={{
                display: 'flex', alignItems: 'center', gap: '10px',
                paddingTop: i > 0 ? '8px' : 0,
                marginTop: i > 0 ? '8px' : 0,
                borderTop: i > 0 ? '1px solid rgba(0,0,0,0.04)' : 'none',
              }}>
                <MiniGauge value={r.gaugeValue} max={r.gaugeMax} color={r.color} size={48} />
                <div style={{ flex: 1 }}>
                  <div style={{ fontSize: '11px', fontWeight: 600, color: '#2D332D' }}>{r.label}</div>
                  <div style={{ fontSize: '9.5px', color: '#6B736B', marginBottom: '3px' }}>{r.desc}</div>
                  <div style={{ display: 'flex', gap: '10px', fontSize: '9.5px' }}>
                    <span>
                      <span style={{ color: '#8A928A' }}>Actual </span>
                      <span style={{ fontWeight: 700, color: '#C62828' }}>{r.displayActual}</span>
                    </span>
                    <span>
                      <span style={{ color: '#8A928A' }}>Required </span>
                      <span style={{ fontWeight: 600, color: '#2D332D' }}>{r.displayThreshold}</span>
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>,
        document.body
      )}
    </>
  );
}

const styles = {
  header: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    cursor: 'pointer', userSelect: 'none',
  },
  headerLeft: { display: 'flex', alignItems: 'center', gap: '14px' },
  title: { fontSize: '13px', fontWeight: 600, color: '#1B5E20', letterSpacing: '0.01em' },
  statPill: {
    display: 'inline-flex', alignItems: 'center', gap: '5px',
    padding: '3px 10px', borderRadius: '10px', fontSize: '11px', fontWeight: 600,
  },
  passedPill: { background: 'rgba(76,175,80,0.10)', color: '#2E7D32' },
  failedPill: { background: 'rgba(239,83,80,0.08)', color: '#C62828' },
  chevron: { fontSize: '14px', color: '#8A928A', fontWeight: 600 },
  body: { overflow: 'hidden' },
  groupLabel: {
    fontSize: '10.5px', fontWeight: 700, textTransform: 'uppercase',
    letterSpacing: '0.08em', marginBottom: '10px',
    display: 'flex', alignItems: 'center', gap: '6px',
  },
  chipGrid: { display: 'flex', flexWrap: 'wrap', gap: '6px' },
  chip: {
    display: 'inline-flex', alignItems: 'center', gap: '4px',
    padding: '4px 10px', borderRadius: '8px',
    fontSize: '10.5px', fontWeight: 500, cursor: 'default',
  },
  passedChip: {
    background: 'rgba(76,175,80,0.07)', color: '#2D332D',
    border: '1px solid rgba(76,175,80,0.15)',
  },
  failedChip: {
    background: 'rgba(239,83,80,0.06)', color: '#C62828',
    border: '1px solid rgba(239,83,80,0.18)',
  },
  dot: { width: '6px', height: '6px', borderRadius: '50%', flexShrink: 0 },
};

function SensorValidationReport() {
  const [data, setData] = useState(null);
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    getBetaSensorValidationReport()
      .then(res => setData(res.data))
      .catch(() => {});
  }, []);

  if (!data || data.total === 0) return null;

  const passed = data.sensors.filter(s => s.passed);
  const failed = data.sensors.filter(s => !s.passed);

  return (
    <GlassCard delay={0} intensity="light" padding="16px 20px">
      <div style={styles.header} onClick={() => setExpanded(e => !e)}>
        <div style={styles.headerLeft}>
          <div style={styles.title}>
            Sensor Validation
            <InfoTooltip text="Initial per-sensor quality checks before subsystem clustering. Sensors must pass data coverage, signal variance, and value diversity thresholds to enter the analysis pipeline." />
          </div>
          <span style={{ ...styles.statPill, ...styles.passedPill }}>
            {data.passed} passed
          </span>
          {data.failed > 0 && (
            <span style={{ ...styles.statPill, ...styles.failedPill }}>
              {data.failed} removed
            </span>
          )}
          <span style={{ fontSize: '11px', color: '#8A928A' }}>
            {data.total} total sensors evaluated
          </span>
        </div>
        <motion.span
          style={styles.chevron}
          animate={{ rotate: expanded ? 180 : 0 }}
          transition={{ duration: 0.25 }}
        >
          &#9662;
        </motion.span>
      </div>

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            style={styles.body}
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.3, ease: [0.4, 0, 0.2, 1] }}
          >
            {/* Removed */}
            {failed.length > 0 && (
              <div style={{ paddingTop: '16px' }}>
                <div style={{ ...styles.groupLabel, color: '#C62828' }}>
                  <span style={{ ...styles.dot, background: '#EF5350' }} />
                  Removed ({failed.length})
                </div>
                <div style={styles.chipGrid}>
                  {failed.map(s => (
                    <span key={s.sensor} style={{ ...styles.chip, ...styles.failedChip }}>
                      <span style={{ ...styles.dot, background: '#EF5350' }} />
                      {formatSensorName(s.sensor)}
                      <FailedInfoButton sensor={s} />
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Passed */}
            <div style={{ paddingTop: '16px' }}>
              <div style={{ ...styles.groupLabel, color: '#2E7D32' }}>
                <span style={{ ...styles.dot, background: '#4CAF50' }} />
                Passed ({passed.length})
              </div>
              <div style={styles.chipGrid}>
                {passed.map(s => (
                  <span key={s.sensor} style={{ ...styles.chip, ...styles.passedChip }}>
                    <span style={{ ...styles.dot, background: '#4CAF50' }} />
                    {formatSensorName(s.sensor)}
                  </span>
                ))}
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </GlassCard>
  );
}

export default SensorValidationReport;
