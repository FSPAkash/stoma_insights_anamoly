import React, { useState } from 'react';
import ReactDOM from 'react-dom';
import { motion, AnimatePresence } from 'framer-motion';
import {
  ResponsiveContainer, LineChart, Line, XAxis, YAxis,
  CartesianGrid, ReferenceArea, Tooltip,
} from 'recharts';
import { formatSensorName, formatScore } from '../utils/formatters';
import { systemColor } from '../utils/formatters';

const SCORE_COLORS = { sqs: '#1976D2', a: '#E65100', b: '#2E7D32' };
const COMPARE_COLORS = ['#1B5E20', '#0D47A1', '#E65100', '#4A148C', '#004D40', '#BF360C', '#1A237E', '#33691E'];

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
    width: '100%', maxWidth: '1100px', maxHeight: '90vh', overflow: 'auto', padding: '32px',
  },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' },
  title: { fontSize: '18px', fontWeight: 600, color: '#1B5E20' },
  closeBtn: {
    width: '36px', height: '36px', borderRadius: '50%', border: '1px solid #CBE6C8',
    background: 'rgba(255,255,255,0.7)', cursor: 'pointer', display: 'flex',
    alignItems: 'center', justifyContent: 'center', fontSize: '16px', color: '#4A524A',
    transition: 'all 0.2s', flexShrink: 0,
  },
  toggleRow: { display: 'flex', gap: '6px', marginBottom: '16px' },
  toggleBtn: (active) => ({
    padding: '6px 16px', borderRadius: '20px', fontSize: '11px', fontWeight: active ? 600 : 500,
    cursor: 'pointer', border: active ? '2px solid #1B5E20' : '1.5px solid rgba(203,230,200,0.6)',
    background: active ? 'rgba(27,94,32,0.08)' : 'rgba(255,255,255,0.6)',
    color: active ? '#1B5E20' : '#6B736B', transition: 'all 0.2s',
  }),
  scoreToggleRow: { display: 'flex', gap: '6px', marginBottom: '16px' },
  legend: { display: 'flex', gap: '14px', justifyContent: 'center', marginBottom: '12px', flexWrap: 'wrap' },
  legendItem: { display: 'flex', alignItems: 'center', gap: '5px', fontSize: '11px', color: '#6B736B' },
  subplotLabel: { fontSize: '11px', fontWeight: 600, marginBottom: '4px', marginLeft: '4px' },
};

const CompareTooltip = ({ active, payload }) => {
  if (!active || !payload || !payload.length) return null;
  const row = payload[0]?.payload;
  return (
    <div style={{
      background: 'rgba(255,255,255,0.94)', backdropFilter: 'blur(12px)',
      border: '1px solid rgba(203,230,200,0.5)', borderRadius: '10px',
      padding: '8px 12px', fontSize: '10px', boxShadow: '0 4px 16px rgba(0,0,0,0.08)',
      maxWidth: '280px',
    }}>
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

function CompareModal({ items, chartData, downtimeBands, onClose, mode = 'sensor' }) {
  const [viewMode, setViewMode] = useState('stacked');
  const [scoreType, setScoreType] = useState('sqs');

  const isSensor = mode === 'sensor';
  const title = isSensor
    ? `Compare ${items.length} Sensors`
    : `Compare ${items.length} Subsystems`;

  const scoreTypes = isSensor
    ? [
        { key: 'sqs', label: 'SQS', color: SCORE_COLORS.sqs },
        { key: 'a', label: 'Engine A', color: SCORE_COLORS.a },
        { key: 'b', label: 'Engine B', color: SCORE_COLORS.b },
      ]
    : [];

  const getDataKey = (item, st) => {
    if (isSensor) return `${item}__${st}`;
    return item;
  };

  const getColor = (item, idx) => {
    if (!isSensor) return systemColor(item, idx);
    return COMPARE_COLORS[idx % COMPARE_COLORS.length];
  };

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
            <div style={styles.title}>{title}</div>
            <motion.button
              style={styles.closeBtn} onClick={onClose}
              whileHover={{ background: '#FFCDD2', borderColor: '#EF9A9A', color: '#C62828' }}
            >x</motion.button>
          </div>

          <div style={{ display: 'flex', gap: '16px', alignItems: 'center', flexWrap: 'wrap', marginBottom: '16px' }}>
            <div style={styles.toggleRow}>
              <div style={styles.toggleBtn(viewMode === 'stacked')} onClick={() => setViewMode('stacked')}>Stacked</div>
              <div style={styles.toggleBtn(viewMode === 'overlay')} onClick={() => setViewMode('overlay')}>Overlay</div>
            </div>

            {isSensor && (
              <div style={styles.scoreToggleRow}>
                {scoreTypes.map(st => (
                  <div key={st.key} style={styles.toggleBtn(scoreType === st.key)} onClick={() => setScoreType(st.key)}>
                    {st.label}
                  </div>
                ))}
              </div>
            )}
          </div>

          <div style={styles.legend}>
            {items.map((item, idx) => (
              <div key={item} style={styles.legendItem}>
                <div style={{ width: 14, height: 3, borderRadius: 2, background: getColor(item, idx) }} />
                {isSensor ? formatSensorName(item) : item.replace('_', ' ')}
              </div>
            ))}
            <div style={styles.legendItem}>
              <div style={{ width: 14, height: 8, borderRadius: 2, background: 'rgba(239,83,80,0.15)' }} />
              Downtime
            </div>
          </div>

          {viewMode === 'overlay' ? (
            <ResponsiveContainer width="100%" height={400}>
              <LineChart data={chartData} margin={{ top: 8, right: 20, bottom: 4, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(203,230,200,0.3)" />
                <XAxis
                  dataKey="_idx" type="number" domain={['dataMin', 'dataMax']}
                  tick={{ fontSize: 9, fill: '#8A928A' }} tickLine={false}
                  tickFormatter={(i) => chartData[i]?.ts || ''}
                />
                <YAxis domain={[0, 1]} tick={{ fontSize: 9, fill: '#8A928A' }} tickLine={false} width={30} ticks={[0, 0.5, 1]} />
                <Tooltip content={<CompareTooltip />} />
                {downtimeBands.map((band, i) => (
                  <ReferenceArea key={i} x1={band.start} x2={band.end} fill="rgba(239,83,80,0.12)" fillOpacity={1} strokeOpacity={0} />
                ))}
                {items.map((item, idx) => (
                  <Line
                    key={item}
                    type="monotone"
                    dataKey={getDataKey(item, scoreType)}
                    stroke={getColor(item, idx)}
                    strokeWidth={1.5}
                    dot={false}
                    connectNulls={false}
                    name={isSensor ? formatSensorName(item) : item.replace('_', ' ')}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div>
              {items.map((item, idx) => {
                const color = getColor(item, idx);
                const label = isSensor ? formatSensorName(item) : item.replace('_', ' ');
                return (
                  <div key={item} style={{ marginBottom: '8px' }}>
                    <div style={{ ...styles.subplotLabel, color }}>{label}</div>
                    <ResponsiveContainer width="100%" height={140}>
                      <LineChart data={chartData} margin={{ top: 4, right: 20, bottom: 0, left: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(203,230,200,0.3)" />
                        <XAxis
                          dataKey="_idx" type="number" domain={['dataMin', 'dataMax']}
                          tick={{ fontSize: 9, fill: '#8A928A' }} tickLine={false}
                          tickFormatter={(i) => chartData[i]?.ts || ''}
                          hide={idx < items.length - 1}
                        />
                        <YAxis domain={[0, 1]} tick={{ fontSize: 9, fill: '#8A928A' }} tickLine={false} width={30} ticks={[0, 0.5, 1]} />
                        <Tooltip content={<CompareTooltip />} />
                        {downtimeBands.map((band, i) => (
                          <ReferenceArea key={i} x1={band.start} x2={band.end} fill="rgba(239,83,80,0.12)" fillOpacity={1} strokeOpacity={0} />
                        ))}
                        {isSensor ? (
                          <>
                            <Line type="monotone" dataKey={`${item}__sqs`} stroke={SCORE_COLORS.sqs} strokeWidth={1.2} dot={false} connectNulls={false} name="SQS" />
                            <Line type="monotone" dataKey={`${item}__a`} stroke={SCORE_COLORS.a} strokeWidth={1.2} dot={false} connectNulls={false} name="Engine A" />
                            <Line type="monotone" dataKey={`${item}__b`} stroke={SCORE_COLORS.b} strokeWidth={1.2} dot={false} connectNulls={false} name="Engine B" />
                          </>
                        ) : (
                          <Line type="monotone" dataKey={item} stroke={color} strokeWidth={1.5} dot={false} connectNulls={false} name={label} />
                        )}
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                );
              })}
            </div>
          )}
        </motion.div>
      </motion.div>
    </AnimatePresence>,
    document.body
  );
}

export default CompareModal;
