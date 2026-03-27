import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import ReactDOM from 'react-dom';
import { motion, AnimatePresence } from 'framer-motion';
import GlassCard from './GlassCard';
import InfoTooltip from './InfoTooltip';
import { getBetaAlerts } from '../utils/api';
import {
  formatDuration,
  formatScore,
  formatSensorName,
  formatTimestamp,
  classColor,
  systemBgColor,
} from '../utils/formatters';

const styles = {
  headerRow: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '16px',
    gap: '12px',
    flexWrap: 'wrap',
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
  controlsStack: {
    display: 'flex',
    flexDirection: 'column',
    gap: '12px',
    marginBottom: '16px',
  },
  primaryControlRow: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    gap: '12px',
    flexWrap: 'wrap',
  },
  secondaryControlRow: {
    display: 'flex',
    gap: '10px',
    flexWrap: 'wrap',
    alignItems: 'center',
  },
  controlGroup: {
    display: 'flex',
    gap: '6px',
    flexWrap: 'wrap',
    alignItems: 'center',
    padding: '6px 8px',
    borderRadius: '14px',
    background: 'rgba(245,247,245,0.72)',
    border: '1px solid rgba(203,230,200,0.45)',
    backdropFilter: 'blur(10px)',
    WebkitBackdropFilter: 'blur(10px)',
  },
  filterLabel: {
    fontSize: '10px',
    fontWeight: 700,
    color: '#8A928A',
    marginRight: '2px',
    textTransform: 'uppercase',
    letterSpacing: '0.06em',
  },
  pill: (active) => ({
    padding: '4px 12px',
    borderRadius: '18px',
    fontSize: '11px',
    fontWeight: 600,
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
  modeToggle: {
    display: 'inline-flex',
    gap: '6px',
    alignItems: 'center',
    flexWrap: 'wrap',
    padding: '8px 10px',
    borderRadius: '16px',
    background: 'rgba(255,255,255,0.72)',
    border: '1px solid rgba(203,230,200,0.5)',
    backdropFilter: 'blur(12px)',
    WebkitBackdropFilter: 'blur(12px)',
  },
  modeLabel: {
    fontSize: '11px',
    fontWeight: 600,
    color: '#8A928A',
    textTransform: 'uppercase',
    letterSpacing: '0.06em',
    marginRight: '4px',
  },
  modePill: (active) => ({
    padding: '6px 14px',
    borderRadius: '19px',
    fontSize: '11px',
    fontWeight: active ? 700 : 600,
    cursor: 'pointer',
    border: active ? '2px solid #2E7D32' : '1.5px solid rgba(203,230,200,0.65)',
    background: active ? 'rgba(76,175,80,0.14)' : 'rgba(255,255,255,0.78)',
    color: active ? '#1B5E20' : '#6B736B',
    transition: 'all 0.2s ease',
    userSelect: 'none',
  }),
  resultBadge: {
    display: 'inline-flex',
    alignItems: 'center',
    padding: '6px 10px',
    borderRadius: '14px',
    background: 'rgba(245,247,245,0.72)',
    border: '1px solid rgba(203,230,200,0.45)',
    color: '#7D877D',
    fontSize: '11px',
    fontWeight: 600,
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
    background: severity === 'HIGH'
      ? 'rgba(239,83,80,0.10)'
      : severity === 'MEDIUM'
        ? 'rgba(255,167,38,0.14)'
        : severity === 'MIXED'
          ? 'rgba(120,144,156,0.16)'
        : 'rgba(255,213,79,0.18)',
    color: severity === 'HIGH'
      ? '#D32F2F'
      : severity === 'MEDIUM'
        ? '#E65100'
        : severity === 'MIXED'
          ? '#455A64'
        : '#9A6A00',
    border: severity === 'HIGH'
      ? '1px solid rgba(239,83,80,0.22)'
      : severity === 'MEDIUM'
        ? '1px solid rgba(255,167,38,0.28)'
        : severity === 'MIXED'
          ? '1px solid rgba(120,144,156,0.30)'
        : '1px solid rgba(255,193,7,0.30)',
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
    gap: '12px',
  },
  detailLabel: { color: '#8A928A' },
  detailValue: { fontWeight: 500, fontVariantNumeric: 'tabular-nums', textAlign: 'right' },
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
    width: `${Math.min((score || 0) * 100, 100)}%`,
    background: severity === 'HIGH'
      ? 'linear-gradient(90deg, rgba(239,83,80,0.88), #D32F2F)'
      : severity === 'MEDIUM'
        ? 'linear-gradient(90deg, rgba(255,167,38,0.9), #F57C00)'
        : severity === 'MIXED'
          ? 'linear-gradient(90deg, rgba(120,144,156,0.9), #455A64)'
        : 'linear-gradient(90deg, rgba(255,213,79,0.95), #FFB300)',
    transition: 'width 0.8s ease',
  }),
  minuteSection: {
    marginTop: '12px',
    borderTop: '1px solid rgba(203,230,200,0.45)',
    paddingTop: '10px',
  },
  minuteTitle: {
    fontSize: '10px',
    fontWeight: 700,
    color: '#6B736B',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    marginBottom: '6px',
  },
  minuteTableWrap: {
    border: '1px solid rgba(203,230,200,0.45)',
    borderRadius: '10px',
    background: 'rgba(255,255,255,0.62)',
    maxHeight: '160px',
    overflowY: 'auto',
  },
  minuteTable: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: '11px',
  },
  minuteTh: {
    position: 'sticky',
    top: 0,
    background: 'rgba(245,248,245,0.98)',
    color: '#7D877D',
    fontWeight: 700,
    textAlign: 'left',
    padding: '6px 8px',
    borderBottom: '1px solid rgba(203,230,200,0.5)',
    whiteSpace: 'nowrap',
  },
  minuteTd: {
    padding: '5px 8px',
    borderBottom: '1px solid rgba(240,244,240,0.9)',
    color: '#2D332D',
    whiteSpace: 'nowrap',
    fontVariantNumeric: 'tabular-nums',
  },
  minuteSeverityBadge: (severity) => ({
    display: 'inline-block',
    padding: '1px 6px',
    borderRadius: '10px',
    fontSize: '10px',
    fontWeight: 700,
    background: severity === 'HIGH'
      ? 'rgba(239,83,80,0.12)'
      : severity === 'MEDIUM'
        ? 'rgba(255,167,38,0.16)'
        : severity === 'MIXED'
          ? 'rgba(120,144,156,0.18)'
          : 'rgba(255,213,79,0.2)',
    color: severity === 'HIGH'
      ? '#C62828'
      : severity === 'MEDIUM'
        ? '#E65100'
        : severity === 'MIXED'
          ? '#455A64'
        : '#9A6A00',
    border: severity === 'HIGH'
      ? '1px solid rgba(239,83,80,0.25)'
      : severity === 'MEDIUM'
        ? '1px solid rgba(255,167,38,0.28)'
        : severity === 'MIXED'
          ? '1px solid rgba(120,144,156,0.32)'
        : '1px solid rgba(255,193,7,0.3)',
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

const getAlertDisplaySeverity = (alert) => {
  if (!alert) return 'LOW';
  const severity = String(alert.severity || '').toUpperCase();
  if (severity) return severity;
  if ((alert.high_count || 0) > 0 && ((alert.medium_count || 0) > 0 || (alert.low_count || 0) > 0)) return 'MIXED';
  if ((alert.high_count || 0) > 0) return 'HIGH';
  if ((alert.medium_count || 0) > 0) return 'MEDIUM';
  return 'LOW';
};

const getSeverityMixLabel = (alert) => {
  if (!alert) return '';
  if (alert.severity_mix) return String(alert.severity_mix);
  const parts = [];
  if (alert.high_count) parts.push(`${alert.high_count} high`);
  if (alert.medium_count) parts.push(`${alert.medium_count} medium`);
  if (alert.low_count) parts.push(`${alert.low_count} low`);
  return parts.join(', ');
};

const alertMatchesSeverity = (alert, filter) => {
  if (filter === 'ALL') return true;
  if ((alert.view_type || 'minute') === 'span') {
    if (filter === 'HIGH') return Number(alert.high_count || 0) > 0;
    if (filter === 'MEDIUM') return Number(alert.medium_count || 0) > 0;
    if (filter === 'LOW') return Number(alert.low_count || 0) > 0;
    if (filter === 'MIXED') return getAlertDisplaySeverity(alert) === 'MIXED';
  }
  return String(alert.severity || '').toUpperCase() === filter;
};

const getAlertSortRank = (alert) => {
  const displaySeverity = getAlertDisplaySeverity(alert);
  const order = { HIGH: 3, MIXED: 2, MEDIUM: 1, LOW: 0 };
  return order[displaySeverity] ?? -1;
};

const timeBadgeStyle = {
  fontSize: '12px', fontWeight: 600, color: '#1B5E20',
  background: 'rgba(129,199,132,0.15)', border: '1px solid rgba(129,199,132,0.3)',
  borderRadius: '8px', padding: '5px 14px', marginLeft: 'auto',
  whiteSpace: 'nowrap', letterSpacing: '0.02em', cursor: 'pointer',
};

const parseUtcMs = (value) => {
  if (value == null) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T');
  const ms = Date.parse(normalized);
  return Number.isNaN(ms) ? null : ms;
};

function BetaAlertEpisodeCards({ onSelectAlert, selectedDay, filterLabel, onFilterClick, selectedSystem, fingerprints: fingerprintsProp }) {
  const [spanAlerts, setSpanAlerts] = useState([]);
  const [minuteAlerts, setMinuteAlerts] = useState([]);
  const [loading, setLoading] = useState(false);
  const [severityFilter, setSeverityFilter] = useState('ALL');
  const [classFilter, setClassFilter] = useState('ALL');
  const [sortBy, setSortBy] = useState('RECENT');
  const [expanded, setExpanded] = useState(false);
  const fingerprints = useMemo(() => (fingerprintsProp || []).filter(f => f.has_alarm && f.event_start), [fingerprintsProp]);
  const headerRef = useRef(null);

  const isPeakAlarm = useCallback((alert) => {
    if (!fingerprints.length) return false;
    const aStart = new Date(alert.start_ts).getTime();
    const aEnd = new Date(alert.end_ts).getTime();
    return fingerprints.some(fp => {
      if (fp.system_id !== alert.class) return false;
      const fStart = new Date(fp.event_start).getTime();
      const fEnd = new Date(fp.event_end).getTime();
      return aStart <= fEnd && aEnd >= fStart;
    });
  }, [fingerprints]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    const params = { class: selectedSystem };
    if (!selectedSystem) delete params.class;
    const spanParams = { ...params, alarm_view: 'span' };
    const minuteParams = { ...params, alarm_view: 'minute' };
    Promise.all([
      getBetaAlerts(spanParams),
      getBetaAlerts(minuteParams),
    ])
      .then(([spanRes, minuteRes]) => {
        if (!cancelled) {
          setSpanAlerts(spanRes?.data?.alerts || []);
          setMinuteAlerts(minuteRes?.data?.alerts || []);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setSpanAlerts([]);
          setMinuteAlerts([]);
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [selectedSystem]);

  useEffect(() => {
    setExpanded(false);
  }, [severityFilter]);

  useEffect(() => {
    setClassFilter(selectedSystem || 'ALL');
    setExpanded(false);
  }, [selectedSystem]);

  const nonNormalAlerts = useMemo(() => {
    if (!spanAlerts || !spanAlerts.length) return [];
    let filtered = spanAlerts.filter((alert) => alert.class !== 'NORMAL');
    if (selectedDay) {
      filtered = filtered.filter((alert) => {
        const startDate = alert.start_ts ? String(alert.start_ts).substring(0, 10) : '';
        const endDate = alert.end_ts ? String(alert.end_ts).substring(0, 10) : '';
        return startDate === selectedDay || endDate === selectedDay;
      });
    }
    return filtered;
  }, [spanAlerts, selectedDay]);

  const minuteNonNormalAlerts = useMemo(() => {
    if (!minuteAlerts || !minuteAlerts.length) return [];
    return minuteAlerts.filter((alert) => alert.class !== 'NORMAL');
  }, [minuteAlerts]);

  const getSpanMinuteRows = useCallback((spanAlert) => {
    const spanStart = parseUtcMs(spanAlert.start_ts || spanAlert.event_start);
    const spanEnd = parseUtcMs(spanAlert.end_ts || spanAlert.event_end || spanAlert.start_ts || spanAlert.event_start);
    if (spanStart == null || spanEnd == null) return [];
    return minuteNonNormalAlerts
      .filter((minuteAlert) => {
        if (minuteAlert.class !== spanAlert.class) return false;
        const minuteStart = parseUtcMs(minuteAlert.start_ts || minuteAlert.event_start);
        const minuteEnd = parseUtcMs(minuteAlert.end_ts || minuteAlert.event_end || minuteAlert.start_ts || minuteAlert.event_start);
        if (minuteStart == null || minuteEnd == null) return false;
        return minuteStart <= spanEnd && minuteEnd >= spanStart;
      })
      .sort((a, b) => {
        const aTs = String(a.start_ts || a.event_start || '');
        const bTs = String(b.start_ts || b.event_start || '');
        return aTs.localeCompare(bTs);
      });
  }, [minuteNonNormalAlerts]);

  const availableClasses = useMemo(() => {
    const classes = new Set();
    nonNormalAlerts.forEach((alert) => {
      if (alert.class) classes.add(alert.class);
    });
    return Array.from(classes).sort();
  }, [nonNormalAlerts]);

  const filteredAlerts = useMemo(() => {
    let result = [...nonNormalAlerts];
    if (severityFilter !== 'ALL') {
      result = result.filter((alert) => alertMatchesSeverity(alert, severityFilter));
    }
    if (classFilter !== 'ALL') {
      result = result.filter((alert) => alert.class === classFilter);
    }
    if (sortBy === 'RECENT') {
      return result.sort((a, b) => {
        const tsA = a.end_ts || a.start_ts || '';
        const tsB = b.end_ts || b.start_ts || '';
        return tsB.localeCompare(tsA);
      });
    }
    return result.sort((a, b) => {
      const delta = getAlertSortRank(b) - getAlertSortRank(a);
      if (delta !== 0) return delta;
      return (b.max_score || 0) - (a.max_score || 0);
    });
  }, [nonNormalAlerts, severityFilter, classFilter, sortBy]);

  const visibleAlerts = expanded ? filteredAlerts : filteredAlerts.slice(0, INITIAL_SHOW);
  const hiddenCount = filteredAlerts.length - INITIAL_SHOW;
  const hasMore = hiddenCount > 0;
  const classFilterLocked = !!selectedSystem;
  const hasActiveFilter = severityFilter !== 'ALL' || (!classFilterLocked && classFilter !== 'ALL');

  const clearFilters = () => {
    setSeverityFilter('ALL');
    setClassFilter(selectedSystem || 'ALL');
    setExpanded(false);
  };

  const handleCollapse = useCallback(() => {
    setExpanded(false);
    if (headerRef.current) {
      headerRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }, []);

  const formatAlarmWindow = useCallback((alert) => {
    const start = alert?.start_ts;
    const end = alert?.end_ts;
    if (!start) return '--';
    if (!end || String(start) === String(end)) return formatTimestamp(start);
    return `${formatTimestamp(start)} - ${formatTimestamp(end)}`;
  }, []);

  return (
    <GlassCard delay={0.55} style={{ marginTop: '8px' }} intensity="normal" padding="20px 24px 28px">
      <div ref={headerRef} style={styles.headerRow}>
        <div style={styles.heading}>
          System Anomalies ({filteredAlerts.length} of {nonNormalAlerts.length})
          <InfoTooltip text="Each card represents one dynamic contiguous anomaly span built from source minute-level subsystem anomalies." />
        </div>
        {filterLabel && <span style={timeBadgeStyle} onClick={onFilterClick}>{filterLabel}</span>}
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

      <div style={styles.controlsStack}>
        <div style={styles.primaryControlRow}>
          <span style={styles.resultBadge}>
            Showing {visibleAlerts.length} of {filteredAlerts.length} anomaly span{filteredAlerts.length !== 1 ? 's' : ''}
          </span>
        </div>

        <div style={styles.secondaryControlRow}>
          <div style={styles.controlGroup}>
            <span style={styles.filterLabel}>Severity</span>
            {['ALL', 'HIGH', 'MEDIUM', 'LOW', 'MIXED'].map((severity) => (
              <motion.span
                key={severity}
                style={styles.pill(severityFilter === severity)}
                onClick={() => setSeverityFilter(severity)}
                whileHover={{ scale: 1.04 }}
                whileTap={{ scale: 0.97 }}
              >
                {severity}
              </motion.span>
            ))}
          </div>

          {classFilterLocked ? (
            <div style={styles.controlGroup}>
              <span style={styles.filterLabel}>Subsystem</span>
              <span style={styles.pill(true)}>{selectedSystem}</span>
            </div>
          ) : (
            <div style={styles.controlGroup}>
              <span style={styles.filterLabel}>Class</span>
              <motion.span
                style={styles.pill(classFilter === 'ALL')}
                onClick={() => setClassFilter('ALL')}
                whileHover={{ scale: 1.04 }}
                whileTap={{ scale: 0.97 }}
              >
                ALL
              </motion.span>
              {availableClasses.map((cls) => (
                <motion.span
                  key={cls}
                  style={styles.pill(classFilter === cls)}
                  onClick={() => setClassFilter(cls)}
                  whileHover={{ scale: 1.04 }}
                  whileTap={{ scale: 0.97 }}
                >
                  {cls}
                </motion.span>
              ))}
            </div>
          )}

          <div style={styles.controlGroup}>
            <span style={styles.filterLabel}>Sort</span>
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
        </div>
      </div>

      <div style={{ position: 'relative' }}>
      {loading && filteredAlerts.length > 0 && (
        <div style={{
          position: 'absolute', top: 0, left: 0, right: 0, bottom: 0,
          background: 'rgba(20, 24, 20, 0.45)', backdropFilter: 'blur(2px)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          zIndex: 10, borderRadius: '8px',
          color: '#C8D6C0', fontSize: '13px', fontWeight: 500, letterSpacing: '0.3px',
        }}>
          Updating anomaly view...
        </div>
      )}
      {!filteredAlerts.length ? (
        <div style={styles.emptyState}>
          {loading
            ? 'Loading system anomalies...'
            : nonNormalAlerts.length === 0
            ? 'No anomaly spans were detected for the selected day.'
            : 'No anomaly spans match the current filters. Try adjusting the filter criteria above.'}
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
                const displaySeverity = getAlertDisplaySeverity(alert);
                const spanMinuteRows = getSpanMinuteRows(alert);
                const minuteCount = spanMinuteRows.length;
                const severityMixLabel = getSeverityMixLabel(alert);
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
                    onClick={() => onSelectAlert && onSelectAlert(alert)}
                    layout
                  >
                    <div style={styles.cardHeaderRow}>
                      <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
                        <span style={styles.severityBadge(displaySeverity)}>{displaySeverity}</span>
                        <span style={styles.classBadge(alert.class)}>{alert.class}</span>
                        {isPeakAlarm(alert) && (
                          <span style={{
                            fontSize: '9px', fontWeight: 700, color: '#fff',
                            background: 'linear-gradient(135deg, #E65100, #FF6D00)',
                            padding: '2px 7px', borderRadius: '8px', letterSpacing: '0.04em',
                            textTransform: 'uppercase', whiteSpace: 'nowrap',
                          }}>Peak Anomaly</span>
                        )}
                      </div>
                    </div>

                    <div style={styles.timeRange}>{formatAlarmWindow(alert)}</div>

                    <div style={styles.detail}>
                      <span style={styles.detailLabel}>Span</span>
                      <span style={styles.detailValue}>{formatDuration(alert.duration_minutes)}</span>
                    </div>
                    <div style={styles.detail}>
                      <span style={styles.detailLabel}>Anomalies In Span</span>
                      <span style={styles.detailValue}>{minuteCount}</span>
                    </div>
                    <div style={styles.detail}>
                      <span style={styles.detailLabel}>Severity Mix</span>
                      <span style={styles.detailValue}>{severityMixLabel || '--'}</span>
                    </div>
                    <div style={styles.detail}>
                      <span style={styles.detailLabel}>Peak Risk Score</span>
                      <span style={styles.detailValue}>
                        {formatScore(alert.peak_risk_score)}
                        {alert.adaptive_threshold != null && (
                          <span style={{ color: '#8A928A', fontWeight: 400, fontSize: '11px' }}>
                            {' / '}{formatScore(alert.adaptive_threshold)} threshold
                          </span>
                        )}
                      </span>
                    </div>
                    {alert.system_confidence != null && (
                      <div style={styles.detail}>
                        <span style={styles.detailLabel}>Confidence</span>
                        <span style={styles.detailValue}>{((alert.system_confidence || 0) * 100).toFixed(0)}%</span>
                      </div>
                    )}
                    <div style={styles.detail}>
                      <span style={styles.detailLabel}>Primary Sensor</span>
                      <span style={styles.detailValue}>{formatSensorName(alert.sensor_id)}</span>
                    </div>

                    <div style={styles.riskBar}>
                      <div style={styles.riskBarFill(
                        alert.adaptive_threshold > 0
                          ? alert.peak_risk_score / alert.adaptive_threshold
                          : 0,
                        displaySeverity
                      )} />
                    </div>

                    <div style={{ marginTop: '10px', display: 'flex', flexWrap: 'wrap', gap: '2px' }}>
                      {sensors.map((sensor, sensorIdx) => (
                        <span key={`${sensor}-${sensorIdx}`} style={styles.sensorTag}>
                          {formatSensorName(sensor.trim())}
                        </span>
                      ))}
                      {alert.affected_sensor_count > 4 && (
                        <span style={{ ...styles.sensorTag, background: 'rgba(203,230,200,0.5)' }}>
                          +{alert.affected_sensor_count - 4} more
                        </span>
                      )}
                    </div>

                    <div style={styles.minuteSection}>
                      <div style={styles.minuteTitle}>Minute-by-minute anomalies ({spanMinuteRows.length})</div>
                      {spanMinuteRows.length === 0 ? (
                        <div style={{ fontSize: '11px', color: '#8A928A' }}>No minute-level rows found for this span.</div>
                      ) : (
                        <div style={styles.minuteTableWrap}>
                          <table style={styles.minuteTable}>
                            <thead>
                              <tr>
                                <th style={styles.minuteTh}>Window (UTC)</th>
                                <th style={styles.minuteTh}>Severity</th>
                                <th style={styles.minuteTh}>Risk Score</th>
                                <th style={styles.minuteTh}>Confidence</th>
                                <th style={styles.minuteTh}>Primary Sensor</th>
                              </tr>
                            </thead>
                            <tbody>
                              {spanMinuteRows.map((minuteRow, rowIdx) => (
                                <tr key={`${minuteRow.start_ts}-${minuteRow.class}-${rowIdx}`}>
                                  <td style={styles.minuteTd}>{formatAlarmWindow(minuteRow)}</td>
                                  <td style={styles.minuteTd}>
                                    <span style={styles.minuteSeverityBadge(getAlertDisplaySeverity(minuteRow))}>
                                      {getAlertDisplaySeverity(minuteRow)}
                                    </span>
                                  </td>
                                  <td style={styles.minuteTd}>
                                    {formatScore(minuteRow.risk_score)}
                                    {minuteRow.adaptive_threshold != null && (
                                      <span style={{ color: '#8A928A', fontWeight: 400, fontSize: '10px' }}>
                                        {' / '}{formatScore(minuteRow.adaptive_threshold)}
                                      </span>
                                    )}
                                  </td>
                                  <td style={styles.minuteTd}>
                                    {minuteRow.system_confidence != null
                                      ? `${((minuteRow.system_confidence || 0) * 100).toFixed(0)}%`
                                      : '--'}
                                  </td>
                                  <td style={styles.minuteTd}>{formatSensorName(minuteRow.sensor_id)}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>

                    <div style={styles.viewMore}>View anomaly span -&gt;</div>
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
                  v
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
                ^
              </motion.span>
              Collapse System Anomaly Spans
            </motion.button>,
            document.body
          )}
        </>
      )}
      </div>
    </GlassCard>
  );
}

export default BetaAlertEpisodeCards;
