import React, { useState, useMemo, useRef, useCallback } from 'react';
import ReactDOM from 'react-dom';
import { motion, AnimatePresence } from 'framer-motion';
import GlassCard from './GlassCard';
import { formatTimestamp, formatDuration, formatSensorName, formatScore, classColor, systemBgColor } from '../utils/formatters';
import InfoTooltip from './InfoTooltip';

const styles = {
  container: {
    marginTop: '8px',
  },
  headerRow: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '16px',
    flexWrap: 'wrap',
    gap: '12px',
  },
  heading: {
    fontSize: '12px',
    fontWeight: 500,
    color: '#6B736B',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    display: 'flex',
    alignItems: 'center',
  },
  filterRow: {
    display: 'flex',
    gap: '8px',
    flexWrap: 'wrap',
    alignItems: 'center',
  },
  filterLabel: {
    fontSize: '11px',
    fontWeight: 500,
    color: '#8A928A',
    marginRight: '4px',
  },
  pill: (active) => ({
    padding: '5px 14px',
    borderRadius: '20px',
    fontSize: '12px',
    fontWeight: 500,
    cursor: 'pointer',
    border: active ? '1.5px solid #388E3C' : '1.5px solid rgba(203,230,200,0.6)',
    background: active ? 'rgba(76, 175, 80, 0.12)' : 'rgba(255,255,255,0.6)',
    color: active ? '#1B5E20' : '#6B736B',
    transition: 'all 0.2s ease',
    backdropFilter: 'blur(8px)',
    WebkitBackdropFilter: 'blur(8px)',
    userSelect: 'none',
  }),
  pillDivider: {
    width: '1px',
    height: '20px',
    background: '#D0D5D0',
    margin: '0 4px',
  },
  resultCount: {
    fontSize: '12px',
    color: '#8A928A',
    fontVariantNumeric: 'tabular-nums',
  },
  grid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fill, minmax(340px, 1fr))',
    gap: '12px',
  },
  card: {
    background: 'rgba(255, 255, 255, 0.65)',
    backdropFilter: 'blur(24px)',
    WebkitBackdropFilter: 'blur(24px)',
    borderRadius: '16px',
    border: '1px solid rgba(203, 230, 200, 0.4)',
    padding: '20px',
    cursor: 'pointer',
    transition: 'all 0.25s ease',
    position: 'relative',
    overflow: 'hidden',
  },
  severityStripe: (severity) => ({
    position: 'absolute',
    top: 0,
    left: 0,
    width: '4px',
    height: '100%',
    background: severity === 'HIGH' ? '#EF5350' : severity === 'MEDIUM' ? '#FFA726' : '#81C784',
    borderRadius: '4px 0 0 4px',
  }),
  cardHeaderRow: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: '12px',
  },
  severityBadge: (severity) => ({
    display: 'inline-block',
    padding: '3px 10px',
    borderRadius: '20px',
    fontSize: '11px',
    fontWeight: 600,
    background: severity === 'HIGH' ? '#FFCDD2' : severity === 'MEDIUM' ? '#FFE0B2' : '#E6F4EA',
    color: severity === 'HIGH' ? '#C62828' : severity === 'MEDIUM' ? '#E65100' : '#1B5E20',
  }),
  classBadge: (cls) => ({
    display: 'inline-block',
    padding: '3px 10px',
    borderRadius: '20px',
    fontSize: '11px',
    fontWeight: 600,
    background: systemBgColor(cls),
    color: classColor(cls),
  }),
  timeRange: {
    fontSize: '13px',
    fontWeight: 500,
    color: '#2D332D',
    marginBottom: '8px',
  },
  detail: {
    fontSize: '12px',
    color: '#4A524A',
    marginBottom: '4px',
    display: 'flex',
    justifyContent: 'space-between',
  },
  detailLabel: { color: '#8A928A' },
  detailValue: { fontWeight: 500, fontVariantNumeric: 'tabular-nums' },
  sensorTag: {
    display: 'inline-block',
    padding: '2px 8px',
    borderRadius: '6px',
    fontSize: '11px',
    fontWeight: 500,
    background: 'rgba(230, 244, 234, 0.7)',
    color: '#1B5E20',
    margin: '2px',
    border: '1px solid rgba(168,220,168,0.3)',
  },
  viewMore: {
    fontSize: '12px',
    fontWeight: 500,
    color: '#388E3C',
    marginTop: '12px',
    display: 'flex',
    alignItems: 'center',
    gap: '4px',
  },
  riskBar: {
    marginTop: '10px',
    height: '4px',
    borderRadius: '2px',
    background: 'rgba(232,236,232,0.6)',
    overflow: 'hidden',
  },
  riskBarFill: (score, severity) => ({
    height: '100%',
    borderRadius: '2px',
    width: `${Math.min(score * 100, 100)}%`,
    background: severity === 'HIGH' ? 'linear-gradient(90deg, #EF5350, #C62828)' : 'linear-gradient(90deg, #FFA726, #E65100)',
    transition: 'width 0.8s ease',
  }),
  emptyState: {
    textAlign: 'center',
    padding: '48px 20px',
    color: '#8A928A',
    fontSize: '14px',
    background: 'rgba(255,255,255,0.4)',
    borderRadius: '14px',
    border: '1px dashed rgba(203,230,200,0.5)',
  },
};

const INITIAL_SHOW = 3;

function AlertEpisodeCards({ alerts, onSelectAlert, selectedDay }) {
  const [severityFilter, setSeverityFilter] = useState('ALL');
  const [classFilter, setClassFilter] = useState('ALL');

  const [sortBy, setSortBy] = useState('RECENT');
  const [expanded, setExpanded] = useState(false);
  const headerRef = useRef(null);

  const nonNormalAlerts = useMemo(() => {
    if (!alerts || !alerts.length) return [];
    let filtered = alerts.filter((a) => a.class !== 'NORMAL');
    if (selectedDay) {
      filtered = filtered.filter((a) => {
        const startDate = a.start_ts ? String(a.start_ts).substring(0, 10) : '';
        const endDate = a.end_ts ? String(a.end_ts).substring(0, 10) : '';
        return startDate === selectedDay || endDate === selectedDay;
      });
    }
    return filtered;
  }, [alerts, selectedDay]);

  const availableClasses = useMemo(() => {
    const classes = new Set();
    nonNormalAlerts.forEach((a) => { if (a.class) classes.add(a.class); });
    return Array.from(classes).sort();
  }, [nonNormalAlerts]);

  const filteredAlerts = useMemo(() => {
    let result = nonNormalAlerts;
    if (severityFilter !== 'ALL') {
      result = result.filter((a) => a.severity === severityFilter);
    }
    if (classFilter !== 'ALL') {
      result = result.filter((a) => a.class === classFilter);
    }

    if (sortBy === 'RECENT') {
      return result.sort((a, b) => {
        const tsA = a.end_ts || a.start_ts || '';
        const tsB = b.end_ts || b.start_ts || '';
        return tsB.localeCompare(tsA);
      });
    }
    return result.sort((a, b) => {
      if (a.severity === 'HIGH' && b.severity !== 'HIGH') return -1;
      if (a.severity !== 'HIGH' && b.severity === 'HIGH') return 1;
      return (b.max_score || 0) - (a.max_score || 0);
    });
  }, [nonNormalAlerts, severityFilter, classFilter, sortBy]);

  const visibleAlerts = expanded ? filteredAlerts : filteredAlerts.slice(0, INITIAL_SHOW);
  const hiddenCount = filteredAlerts.length - INITIAL_SHOW;
  const hasMore = hiddenCount > 0;

  const clearFilters = () => {
    setSeverityFilter('ALL');
    setClassFilter('ALL');
    setExpanded(false);
  };

  const handleCollapse = useCallback(() => {
    setExpanded(false);
    if (headerRef.current) {
      headerRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }, []);

  const hasActiveFilter = severityFilter !== 'ALL' || classFilter !== 'ALL';

  return (
    <GlassCard delay={0.55} style={{ marginTop: '8px' }} intensity="normal" padding="20px 24px 28px">
      <div ref={headerRef} style={styles.headerRow}>
        <div style={styles.heading}>
          Alert Episodes ({filteredAlerts.length} of {nonNormalAlerts.length})
          <InfoTooltip text="Alert episodes are built by thresholding risk_score at medium (0.55) and high (0.80). Contiguous above-threshold periods are merged if gaps are <= 3 minutes. Episodes < 5 minutes are dropped. Each episode is classified by the mode of per-timestamp fault labels within the window. Only anomalous (non-NORMAL) episodes are shown. Severity is HIGH if peak risk >= 0.85, else MEDIUM." />
        </div>
        {hasActiveFilter && (
          <motion.button
            onClick={clearFilters}
            style={{
              padding: '4px 12px',
              borderRadius: '16px',
              fontSize: '11px',
              fontWeight: 500,
              background: 'rgba(239,83,80,0.08)',
              color: '#C62828',
              border: '1px solid rgba(239,83,80,0.2)',
              cursor: 'pointer',
            }}
            whileHover={{ background: 'rgba(239,83,80,0.15)' }}
            whileTap={{ scale: 0.97 }}
          >
            Clear Filters
          </motion.button>
        )}
      </div>

      <div style={styles.filterRow}>
        <span style={styles.filterLabel}>Severity:</span>
        {['ALL', 'HIGH', 'MEDIUM'].map((s) => (
          <motion.span
            key={s}
            style={styles.pill(severityFilter === s)}
            onClick={() => setSeverityFilter(s)}
            whileHover={{ scale: 1.04 }}
            whileTap={{ scale: 0.97 }}
          >
            {s}
          </motion.span>
        ))}

        <div style={styles.pillDivider} />

        <span style={styles.filterLabel}>Class:</span>
        <motion.span
          style={styles.pill(classFilter === 'ALL')}
          onClick={() => setClassFilter('ALL')}
          whileHover={{ scale: 1.04 }}
          whileTap={{ scale: 0.97 }}
        >
          ALL
        </motion.span>
        {availableClasses.map((c) => (
          <motion.span
            key={c}
            style={styles.pill(classFilter === c)}
            onClick={() => setClassFilter(c)}
            whileHover={{ scale: 1.04 }}
            whileTap={{ scale: 0.97 }}
          >
            {c}
          </motion.span>
        ))}

        <div style={styles.pillDivider} />

        <span style={styles.filterLabel}>Sort:</span>
        <motion.span
          style={styles.pill(sortBy === 'RECENT')}
          onClick={() => setSortBy('RECENT')}
          whileHover={{ scale: 1.04 }}
          whileTap={{ scale: 0.97 }}
        >
          Recent
        </motion.span>
        <motion.span
          style={styles.pill(sortBy === 'SEVERITY')}
          onClick={() => setSortBy('SEVERITY')}
          whileHover={{ scale: 1.04 }}
          whileTap={{ scale: 0.97 }}
        >
          Severity
        </motion.span>
      </div>

      <div style={{ marginTop: '6px', marginBottom: '16px' }}>
        <span style={styles.resultCount}>
          Showing {visibleAlerts.length} of {filteredAlerts.length} episode{filteredAlerts.length !== 1 ? 's' : ''}
        </span>
      </div>

      {!filteredAlerts.length ? (
        <div style={styles.emptyState}>
          {nonNormalAlerts.length === 0
            ? 'No anomalous alert episodes detected. All sensor behavior is within normal bounds.'
            : 'No episodes match the current filters. Try adjusting the filter criteria above.'}
        </div>
      ) : (
        <>
          <div style={styles.grid}>
            <AnimatePresence mode="popLayout">
              {visibleAlerts.map((alert, idx) => {
                const sensors = alert.affected_sensors
                  ? String(alert.affected_sensors).split('|').slice(0, 4)
                  : [];
                const key = `${alert.start_ts}-${alert.end_ts}-${alert.class}-${idx}`;

                return (
                  <motion.div
                    key={key}
                    style={styles.card}
                    initial={{ opacity: 0, y: 12, scale: 0.97 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    exit={{ opacity: 0, y: -8, scale: 0.97 }}
                    transition={{ delay: Math.min(idx * 0.03, 0.3), duration: 0.35 }}
                    whileHover={{
                      boxShadow: '0 12px 36px rgba(27,94,32,0.12)',
                      borderColor: 'rgba(129, 199, 132, 0.55)',
                      y: -3,
                      background: 'rgba(255,255,255,0.78)',
                    }}
                    onClick={() => onSelectAlert(alert)}
                    layout
                  >
                    <div style={styles.severityStripe(alert.severity)} />

                    <div style={styles.cardHeaderRow}>
                      <div style={{ display: 'flex', gap: '6px' }}>
                        <span style={styles.severityBadge(alert.severity)}>{alert.severity}</span>
                        <span style={styles.classBadge(alert.class)}>{alert.class}</span>
                      </div>


                    </div>

                    <div style={styles.timeRange}>
                      {formatTimestamp(alert.start_ts)} - {formatTimestamp(alert.end_ts)}
                    </div>

                    <div style={styles.detail}>
                      <span style={styles.detailLabel}>Duration</span>
                      <span style={styles.detailValue}>{formatDuration(alert.duration_minutes)}</span>
                    </div>
                    <div style={styles.detail}>
                      <span style={styles.detailLabel}>Peak Risk</span>
                      <span style={styles.detailValue}>{formatScore(alert.max_score)}</span>
                    </div>
                    <div style={styles.detail}>
                      <span style={styles.detailLabel}>Mean Risk</span>
                      <span style={styles.detailValue}>{formatScore(alert.mean_score)}</span>
                    </div>
                    <div style={styles.detail}>
                      <span style={styles.detailLabel}>Primary Sensor</span>
                      <span style={styles.detailValue}>{formatSensorName(alert.sensor_id)}</span>
                    </div>

                    <div style={styles.riskBar}>
                      <div style={styles.riskBarFill(alert.max_score || 0, alert.severity)} />
                    </div>

                    <div style={{ marginTop: '10px', display: 'flex', flexWrap: 'wrap', gap: '2px' }}>
                      {sensors.map((s, i) => (
                        <span key={i} style={styles.sensorTag}>{formatSensorName(s.trim())}</span>
                      ))}
                      {alert.affected_sensor_count > 4 && (
                        <span style={{ ...styles.sensorTag, background: 'rgba(203,230,200,0.5)' }}>
                          +{alert.affected_sensor_count - 4} more
                        </span>
                      )}
                    </div>

                    <div style={styles.viewMore}>View Decomposition &#8594;</div>
                  </motion.div>
                );
              })}
            </AnimatePresence>
          </div>

          {hasMore && !expanded && (
            <div style={{ display: 'flex', justifyContent: 'center', marginTop: '20px' }}>
              <motion.button
                onClick={() => setExpanded(true)}
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '6px',
                  padding: '6px 20px',
                  borderRadius: '20px',
                  border: '1px solid rgba(203,230,200,0.6)',
                  background: 'rgba(255,255,255,0.7)',
                  backdropFilter: 'blur(12px)',
                  color: '#388E3C',
                  fontSize: '12px',
                  fontWeight: 500,
                  cursor: 'pointer',
                  boxShadow: '0 4px 16px rgba(27,94,32,0.08)',
                }}
                whileHover={{
                  background: 'rgba(230,244,234,0.8)',
                  borderColor: '#81C784',
                  boxShadow: '0 6px 20px rgba(27,94,32,0.12)',
                  y: -2,
                }}
                whileTap={{ scale: 0.96 }}
              >
                +{hiddenCount} more
                <motion.span
                  animate={{ y: [0, 3, 0] }}
                  transition={{ repeat: Infinity, duration: 1.5, ease: 'easeInOut' }}
                  style={{ fontSize: '10px' }}
                >
                  &#9660;
                </motion.span>
              </motion.button>
            </div>
          )}

          {expanded && hasMore && ReactDOM.createPortal(
            <motion.button
              onClick={handleCollapse}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: 20 }}
              style={{
                position: 'fixed',
                bottom: '28px',
                left: '50%',
                transform: 'translateX(-50%)',
                display: 'inline-flex',
                alignItems: 'center',
                gap: '6px',
                padding: '8px 24px',
                borderRadius: '24px',
                border: '1px solid rgba(203,230,200,0.6)',
                background: 'rgba(255,255,255,0.92)',
                backdropFilter: 'blur(16px)',
                WebkitBackdropFilter: 'blur(16px)',
                color: '#388E3C',
                fontSize: '12px',
                fontWeight: 500,
                cursor: 'pointer',
                boxShadow: '0 8px 32px rgba(27,94,32,0.15), 0 2px 8px rgba(0,0,0,0.06)',
                zIndex: 9999,
              }}
              whileHover={{
                background: 'rgba(230,244,234,0.95)',
                borderColor: '#81C784',
                boxShadow: '0 10px 36px rgba(27,94,32,0.2)',
              }}
              whileTap={{ scale: 0.96 }}
            >
              <motion.span
                animate={{ y: [0, -3, 0] }}
                transition={{ repeat: Infinity, duration: 1.5, ease: 'easeInOut' }}
                style={{ fontSize: '10px' }}
              >
                &#9650;
              </motion.span>
              Collapse Episodes
            </motion.button>,
            document.body
          )}
        </>
      )}
    </GlassCard>
  );
}

export default AlertEpisodeCards;
