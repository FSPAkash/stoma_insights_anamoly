import React, { useMemo } from 'react';
import ReactDOM from 'react-dom';
import { motion, AnimatePresence } from 'framer-motion';
import {
  ResponsiveContainer, LineChart, Line, XAxis, YAxis,
  CartesianGrid, ReferenceArea, Tooltip,
} from 'recharts';
import { formatScore } from '../utils/formatters';
import { systemColor } from '../utils/formatters';

const styles = {
  overlay: {
    position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
    background: 'rgba(26,31,26,0.4)', backdropFilter: 'blur(16px)',
    WebkitBackdropFilter: 'blur(16px)', zIndex: 1100,
    display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px',
  },
  modal: {
    background: 'rgba(255,255,255,0.88)', backdropFilter: 'blur(32px)',
    WebkitBackdropFilter: 'blur(32px)', borderRadius: '24px',
    border: '1px solid rgba(203,230,200,0.5)',
    boxShadow: '0 32px 100px rgba(27,94,32,0.2), 0 8px 32px rgba(27,94,32,0.08)',
    width: '100%', maxWidth: '800px', maxHeight: '90vh', overflow: 'auto', padding: '32px',
  },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '24px' },
  title: { fontSize: '18px', fontWeight: 600, color: '#1B5E20' },
  subtitle: { fontSize: '12px', color: '#8A928A', marginTop: '2px' },
  closeBtn: {
    width: '36px', height: '36px', borderRadius: '50%', border: '1px solid #CBE6C8',
    background: 'rgba(255,255,255,0.7)', cursor: 'pointer', display: 'flex',
    alignItems: 'center', justifyContent: 'center', fontSize: '16px', color: '#4A524A',
    transition: 'all 0.2s', flexShrink: 0,
  },
  metaGrid: {
    display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))',
    gap: '10px', marginTop: '20px',
  },
  metaItem: {
    padding: '12px 14px', background: 'rgba(245,247,245,0.7)',
    backdropFilter: 'blur(8px)', borderRadius: '12px', border: '1px solid rgba(232,236,232,0.6)',
  },
  metaLabel: { fontSize: '10px', color: '#8A928A', marginBottom: '3px', textTransform: 'uppercase', letterSpacing: '0.04em' },
  metaValue: { fontSize: '14px', fontWeight: 600, color: '#2D332D', fontVariantNumeric: 'tabular-nums' },
};

const ChartTooltip = ({ active, payload }) => {
  if (!active || !payload || !payload.length) return null;
  const row = payload[0]?.payload;
  return (
    <div style={{
      background: 'rgba(255,255,255,0.94)', backdropFilter: 'blur(12px)',
      border: '1px solid rgba(203,230,200,0.5)', borderRadius: '10px',
      padding: '8px 12px', fontSize: '10px', boxShadow: '0 4px 16px rgba(0,0,0,0.08)',
    }}>
      {row?.downtime === 1 && (
        <div style={{ background: 'rgba(0,0,0,0.04)', border: '1px solid rgba(0,0,0,0.12)', borderRadius: '4px', padding: '2px 6px', marginBottom: '4px', fontSize: '9px', fontWeight: 600, color: '#616161' }}>
          Downtime
        </div>
      )}
      <div style={{ fontWeight: 600, color: '#1B5E20', marginBottom: '4px' }}>{row?.ts_full || row?.ts || ''}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ display: 'flex', justifyContent: 'space-between', gap: '12px' }}>
          <span style={{ color: p.color }}>{p.name}</span>
          <span style={{ fontWeight: 500, fontVariantNumeric: 'tabular-nums' }}>
            {p.value != null ? Number(p.value).toFixed(3) : '--'}
          </span>
        </div>
      ))}
    </div>
  );
};

function SubsystemDetailModal({ system, systemIndex, chartData, downtimeBands, onClose }) {
  const color = systemColor(system, systemIndex);

  const stats = useMemo(() => {
    if (!chartData?.length || !system) return null;
    const vals = chartData.map(r => r[system]).filter(v => v != null && !isNaN(v));
    if (!vals.length) return null;
    const min = Math.min(...vals);
    const max = Math.max(...vals);
    const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
    const aboveHalf = vals.filter(v => v > 0.5).length;
    const dtCount = chartData.filter(r => r.downtime === 1).length;
    return {
      min, max, mean,
      aboveHalfPct: ((aboveHalf / vals.length) * 100).toFixed(1),
      downtimePct: ((dtCount / chartData.length) * 100).toFixed(1),
      dataPoints: chartData.length,
    };
  }, [chartData, system]);

  if (!system) return null;

  return ReactDOM.createPortal(
    <AnimatePresence>
      <motion.div
        style={styles.overlay}
        initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
        onClick={onClose}
      >
        <motion.div
          style={styles.modal}
          initial={{ opacity: 0, scale: 0.94, y: 24 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.94, y: 24 }}
          transition={{ duration: 0.35, ease: [0.4, 0, 0.2, 1] }}
          onClick={(e) => e.stopPropagation()}
        >
          <div style={styles.header}>
            <div>
              <div style={styles.title}>{system.replace('_', ' ')}</div>
              <div style={styles.subtitle}>Subsystem Fused Score</div>
            </div>
            <motion.button
              style={styles.closeBtn} onClick={onClose}
              whileHover={{ background: '#FFCDD2', borderColor: '#EF9A9A', color: '#C62828' }}
            >x</motion.button>
          </div>

          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={chartData} margin={{ top: 8, right: 20, bottom: 4, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(203,230,200,0.3)" />
              <XAxis
                dataKey="_idx" type="number" domain={['dataMin', 'dataMax']}
                tick={{ fontSize: 9, fill: '#8A928A' }} tickLine={false}
                tickFormatter={(i) => chartData[i]?.ts || ''}
              />
              <YAxis domain={[0, 1]} tick={{ fontSize: 9, fill: '#8A928A' }} tickLine={false} width={30} ticks={[0, 0.25, 0.5, 0.75, 1]} />
              <Tooltip content={<ChartTooltip />} />
              {downtimeBands.map((band, i) => (
                <ReferenceArea key={i} x1={band.start} x2={band.end} fill="rgba(239,83,80,0.12)" fillOpacity={1} strokeOpacity={0} />
              ))}
              <Line type="monotone" dataKey={system} stroke={color} strokeWidth={2} dot={false} connectNulls={false} name={system.replace('_', ' ')} />
            </LineChart>
          </ResponsiveContainer>

          {stats && (
            <div style={styles.metaGrid}>
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>Score Range</div>
                <div style={styles.metaValue}>{formatScore(stats.min)} - {formatScore(stats.max)}</div>
              </div>
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>Mean Score</div>
                <div style={styles.metaValue}>{formatScore(stats.mean)}</div>
              </div>
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>Time Above 0.5</div>
                <div style={styles.metaValue}>{stats.aboveHalfPct}%</div>
              </div>
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>Downtime</div>
                <div style={styles.metaValue}>{stats.downtimePct}%</div>
              </div>
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>Data Points</div>
                <div style={styles.metaValue}>{stats.dataPoints}</div>
              </div>
            </div>
          )}
        </motion.div>
      </motion.div>
    </AnimatePresence>,
    document.body
  );
}

export default SubsystemDetailModal;
