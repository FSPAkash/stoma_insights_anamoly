import React, { useState, useEffect, useRef, useCallback } from 'react';
import ReactDOM from 'react-dom';
import { motion, AnimatePresence } from 'framer-motion';
import GlassCard from './GlassCard';
import InfoTooltip from './InfoTooltip';
import { getBetaSensorValidationReport } from '../utils/api';
import { formatSensorName } from '../utils/formatters';

function parseReasonText(raw) {
  if (!raw) return 'Removed by validation checks';
  const humanReadable = raw
    .split(' | ')
    .map((chunk) => chunk.trim())
    .filter(Boolean)
    .map((chunk) => {
      if (chunk.startsWith('HIGH_MISSINGNESS')) return 'Too much missing data';
      if (chunk.startsWith('CONSTANT_SIGNAL')) return 'No meaningful change detected';
      if (chunk.startsWith('NEAR_CONSTANT')) return 'Almost all readings identical';
      return chunk.replace(/_/g, ' ').toLowerCase();
    });

  return [...new Set(humanReadable)].join(' | ') || 'Removed by validation checks';
}

function FailedInfoButton({ sensor }) {
  const [visible, setVisible] = useState(false);
  const [pos, setPos] = useState({ top: 0, left: 0 });
  const triggerRef = useRef(null);
  const tooltipRef = useRef(null);
  const reasonText = parseReasonText(sensor.removal_reasons);

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
            border: '1px solid rgba(203,230,200,0.6)', borderRadius: '12px',
            padding: '8px 12px', boxShadow: '0 8px 24px rgba(27,94,32,0.14), 0 3px 10px rgba(27,94,32,0.08)',
            width: 'max-content', maxWidth: 'min(90vw, 360px)', pointerEvents: 'none',
          }}
        >
          <div
            title={reasonText}
            style={{
              fontSize: '11px',
              color: '#6B736B',
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              maxWidth: '100%',
            }}
          >
            {reasonText}
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

function SensorValidationReport({ hasData = true }) {
  const [data, setData] = useState(null);
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    if (!hasData) { setData(null); return; }
    getBetaSensorValidationReport()
      .then(res => setData(res.data))
      .catch(() => {});
  }, [hasData]);

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
