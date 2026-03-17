import React, { useState, useEffect, useMemo } from 'react';
import { motion } from 'framer-motion';
import GlassCard from './GlassCard';
import InfoTooltip from './InfoTooltip';
import Sparkline from './Sparkline';
import SubsystemDetailModal from './SubsystemDetailModal';
import CompareModal from './CompareModal';
import { getBetaSubsystemScores } from '../utils/api';
import { systemColor } from '../utils/formatters';

const styles = {
  heading: {
    fontSize: '12px', fontWeight: 500, color: '#6B736B', textTransform: 'uppercase',
    letterSpacing: '0.05em', marginBottom: '16px', display: 'flex', alignItems: 'center',
  },
  toolbar: {
    display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '12px',
  },
  compareBtn: (enabled) => ({
    padding: '6px 14px', borderRadius: '10px', fontSize: '11px', fontWeight: 600,
    cursor: enabled ? 'pointer' : 'default',
    background: enabled ? 'linear-gradient(135deg, #1B5E20 0%, #388E3C 100%)' : 'rgba(203,230,200,0.3)',
    color: enabled ? '#fff' : '#8A928A', border: 'none', transition: 'all 0.2s',
    opacity: enabled ? 1 : 0.6,
  }),
  cardGrid: {
    display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '10px',
  },
  card: (isSelected) => ({
    background: isSelected ? 'rgba(129,199,132,0.12)' : 'rgba(255,255,255,0.45)',
    borderRadius: '14px',
    border: isSelected ? '1.5px solid rgba(129,199,132,0.4)' : '1px solid rgba(203,230,200,0.3)',
    padding: '12px 14px',
    cursor: 'pointer',
    transition: 'all 0.15s',
    position: 'relative',
  }),
  cardHeader: {
    display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px',
  },
  checkbox: { width: '14px', height: '14px', borderRadius: '3px', cursor: 'pointer', accentColor: '#1B5E20', flexShrink: 0 },
  systemName: {
    fontSize: '11px', fontWeight: 600, color: '#4A524A',
    whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', flex: 1,
    display: 'flex', alignItems: 'center', gap: '6px',
  },
  colorDot: (color) => ({ width: '8px', height: '8px', borderRadius: '50%', background: color, flexShrink: 0 }),
  sparklineWrap: { marginBottom: '8px' },
  statsRow: {
    display: 'flex', gap: '6px', justifyContent: 'space-between',
  },
  scorePill: (color) => ({
    fontSize: '10px', fontWeight: 600, fontVariantNumeric: 'tabular-nums',
    padding: '2px 8px', borderRadius: '6px',
    background: `${color}12`, color: color,
    flex: 1, textAlign: 'center',
  }),
  pillLabel: {
    fontSize: '8px', fontWeight: 500, color: '#8A928A', textTransform: 'uppercase',
    letterSpacing: '0.04em', textAlign: 'center', marginBottom: '1px',
  },
  loadingOverlay: {
    display: 'flex', alignItems: 'center', justifyContent: 'center',
    height: '200px', color: '#8A928A', fontSize: '13px',
  },
  emptyState: { textAlign: 'center', padding: '48px 20px', color: '#8A928A', fontSize: '14px' },
};

function getLatestValue(chartData, key) {
  for (let i = chartData.length - 1; i >= 0; i--) {
    const v = chartData[i][key];
    if (v != null && !isNaN(v)) return Number(v);
  }
  return null;
}

function getTrend(chartData, key) {
  let prev = null, last = null;
  for (let i = chartData.length - 1; i >= 0; i--) {
    const v = chartData[i][key];
    if (v != null && !isNaN(v)) {
      if (last === null) last = Number(v);
      else { prev = Number(v); break; }
    }
  }
  if (prev === null || last === null) return null;
  if (last > prev + 0.01) return 'up';
  if (last < prev - 0.01) return 'down';
  return 'flat';
}

function SubsystemScoresStacked({ filterLabel, onFilterClick }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState(new Set());
  const [detailSystem, setDetailSystem] = useState(null);
  const [showCompare, setShowCompare] = useState(false);

  useEffect(() => {
    setLoading(true);
    getBetaSubsystemScores(1)
      .then(res => setData(res.data))
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, []);

  const systems = data?.systems || [];

  const chartData = useMemo(() => {
    if (!data?.timeseries?.length) return [];
    return data.timeseries.map((row, i) => ({
      ...row, _idx: i,
      ts_full: row.ts ? String(row.ts).substring(0, 19).replace('T', ' ') : '',
      ts: row.ts ? String(row.ts).substring(11, 16) : '',
    }));
  }, [data]);

  const downtimeBands = useMemo(() => {
    if (!chartData.length) return [];
    const bands = [];
    let start = null;
    for (let i = 0; i < chartData.length; i++) {
      if (chartData[i].downtime === 1) { if (start === null) start = i; }
      else { if (start !== null) { bands.push({ start, end: i - 1 }); start = null; } }
    }
    if (start !== null) bands.push({ start, end: chartData.length - 1 });
    return bands;
  }, [chartData]);

  const toggleSelect = (sys, e) => {
    e.stopPropagation();
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(sys)) next.delete(sys);
      else next.add(sys);
      return next;
    });
  };

  const filterBadgeStyle = {
    fontSize: '10px', fontWeight: 500, color: '#1B5E20',
    background: 'rgba(129,199,132,0.15)', border: '1px solid rgba(129,199,132,0.3)',
    borderRadius: '6px', padding: '3px 10px', marginLeft: 'auto',
    whiteSpace: 'nowrap', letterSpacing: '0.02em', textTransform: 'none',
  };

  return (
    <GlassCard delay={0.45} style={{ marginTop: '8px' }} intensity="strong">
      <div style={styles.heading}>
        Subsystem Scores ({systems.length} system{systems.length !== 1 ? 's' : ''})
        <InfoTooltip text="Per-subsystem anomaly scores over time. Click any system to see the full chart. Select multiple and click Compare to view side-by-side." />
        {filterLabel && <span style={{ ...filterBadgeStyle, cursor: 'pointer' }} onClick={onFilterClick}>{filterLabel}</span>}
      </div>

      {loading ? (
        <div style={styles.loadingOverlay}>Loading subsystem scores...</div>
      ) : !chartData.length || !systems.length ? (
        <div style={styles.emptyState}>No subsystem score data available.</div>
      ) : (
        <>
          <div style={styles.toolbar}>
            <button
              style={styles.compareBtn(selected.size >= 2)}
              onClick={() => selected.size >= 2 && setShowCompare(true)}
            >
              Compare ({selected.size})
            </button>
            {selected.size > 0 && (
              <span
                style={{ fontSize: '11px', color: '#6B736B', cursor: 'pointer', textDecoration: 'underline' }}
                onClick={() => setSelected(new Set())}
              >Clear</span>
            )}
          </div>

          <div style={styles.cardGrid}>
            {systems.map((sys, idx) => {
              const color = systemColor(sys, idx);
              const latest = getLatestValue(chartData, sys);
              const trend = getTrend(chartData, sys);
              const isSelected = selected.has(sys);
              const scoreCol = latest !== null && latest >= 0.7 ? '#C62828' : latest !== null && latest >= 0.4 ? '#E65100' : '#2E7D32';
              const trendCol = trend === 'up' ? '#C62828' : '#2E7D32';
              return (
                <motion.div
                  key={sys}
                  style={styles.card(isSelected)}
                  whileHover={{
                    background: 'rgba(168,220,168,0.18)',
                    boxShadow: '0 4px 16px rgba(27,94,32,0.08)',
                    borderColor: 'rgba(129,199,132,0.5)',
                    y: -2,
                  }}
                  transition={{ duration: 0.15 }}
                  onClick={() => setDetailSystem(sys)}
                >
                  <div style={styles.cardHeader}>
                    <input
                      type="checkbox"
                      style={styles.checkbox}
                      checked={isSelected}
                      onChange={(e) => toggleSelect(sys, e)}
                      onClick={(e) => e.stopPropagation()}
                    />
                    <div style={styles.systemName}>
                      <div style={styles.colorDot(color)} />
                      {sys.replace('_', ' ')}
                    </div>
                  </div>
                  <div style={styles.sparklineWrap}>
                    <Sparkline data={chartData} dataKey={sys} color={color} downtimeBands={downtimeBands} width="100%" height={36} />
                  </div>
                  <div style={styles.statsRow}>
                    <div>
                      <div style={styles.pillLabel}>Latest</div>
                      <div style={styles.scorePill(scoreCol)}>
                        {latest !== null ? latest.toFixed(3) : '--'}
                      </div>
                    </div>
                    <div>
                      <div style={styles.pillLabel}>Direction</div>
                      <div style={styles.scorePill(trendCol)}>
                        {trend === 'up' ? 'Rising' : trend === 'down' ? 'Falling' : 'Stable'}
                      </div>
                    </div>
                  </div>
                </motion.div>
              );
            })}
          </div>
        </>
      )}

      {detailSystem && (
        <SubsystemDetailModal
          system={detailSystem}
          systemIndex={systems.indexOf(detailSystem)}
          chartData={chartData}
          downtimeBands={downtimeBands}
          onClose={() => setDetailSystem(null)}
        />
      )}

      {showCompare && selected.size >= 2 && (
        <CompareModal
          items={Array.from(selected)}
          chartData={chartData}
          downtimeBands={downtimeBands}
          onClose={() => setShowCompare(false)}
          mode="subsystem"
        />
      )}
    </GlassCard>
  );
}

export default SubsystemScoresStacked;
