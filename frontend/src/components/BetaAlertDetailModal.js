import React, { useEffect, useMemo, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  getBetaAlertsSensorLevel,
  getBetaAlerts,
  getBetaRiskDecompositionForEpisode,
  getBetaStandaloneAlarmDetail,
} from '../utils/api';
import {
  formatDuration,
  formatScore,
  formatSensorName,
  formatTimestamp,
} from '../utils/formatters';
import GaugeWidget from './GaugeWidget';
import InfoTooltip from './InfoTooltip';
import SensorFlowDecomposition from './SensorFlowDecomposition';

const styles = {
  overlay: {
    position: 'fixed',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: 'rgba(26, 31, 26, 0.4)',
    backdropFilter: 'blur(16px)',
    WebkitBackdropFilter: 'blur(16px)',
    zIndex: 1100,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '20px',
  },
  modal: {
    background: 'rgba(255, 255, 255, 0.88)',
    backdropFilter: 'blur(32px)',
    WebkitBackdropFilter: 'blur(32px)',
    borderRadius: '24px',
    border: '1px solid rgba(203, 230, 200, 0.5)',
    boxShadow: '0 32px 100px rgba(27,94,32,0.2), 0 8px 32px rgba(27,94,32,0.08)',
    width: '100%',
    maxWidth: '920px',
    maxHeight: '90vh',
    overflow: 'auto',
    padding: '36px',
  },
  header: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: '28px',
    gap: '18px',
  },
  titleArea: { display: 'flex', flexDirection: 'column', gap: '6px' },
  title: {
    fontSize: '22px',
    fontWeight: 600,
    color: '#1B5E20',
    letterSpacing: '-0.02em',
    display: 'flex',
    alignItems: 'center',
  },
  subtitle: { fontSize: '14px', color: '#6B736B', fontWeight: 400 },
  closeBtn: {
    width: '36px',
    height: '36px',
    borderRadius: '50%',
    border: '1px solid #CBE6C8',
    background: 'rgba(255,255,255,0.7)',
    backdropFilter: 'blur(8px)',
    cursor: 'pointer',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontSize: '16px',
    color: '#4A524A',
    transition: 'all 0.2s',
    flexShrink: 0,
  },
  badges: {
    display: 'flex',
    gap: '8px',
    marginBottom: '20px',
    flexWrap: 'wrap',
  },
  badge: (bg, color) => ({
    display: 'inline-block',
    padding: '4px 12px',
    borderRadius: '20px',
    fontSize: '12px',
    fontWeight: 600,
    background: bg,
    color,
  }),
  metaGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))',
    gap: '12px',
    marginBottom: '24px',
  },
  metaItem: {
    padding: '14px 16px',
    background: 'rgba(245,247,245,0.7)',
    backdropFilter: 'blur(8px)',
    borderRadius: '12px',
    border: '1px solid rgba(232,236,232,0.6)',
  },
  metaLabel: {
    fontSize: '11px',
    color: '#8A928A',
    marginBottom: '4px',
    textTransform: 'uppercase',
    letterSpacing: '0.04em',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
  },
  metaValue: {
    fontSize: '16px',
    fontWeight: 600,
    color: '#2D332D',
    fontVariantNumeric: 'tabular-nums',
  },
  sectionTitle: {
    fontSize: '12px',
    fontWeight: 500,
    color: '#6B736B',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    marginBottom: '12px',
    marginTop: '28px',
    paddingBottom: '8px',
    borderBottom: '1px solid rgba(203,230,200,0.4)',
    display: 'flex',
    alignItems: 'center',
  },
  sensorTable: {
    width: '100%',
    borderCollapse: 'separate',
    borderSpacing: '0 4px',
    fontSize: '12px',
  },
  th: {
    textAlign: 'left',
    padding: '8px 12px',
    color: '#8A928A',
    fontWeight: 500,
    fontSize: '11px',
    textTransform: 'uppercase',
    letterSpacing: '0.04em',
  },
  td: {
    padding: '10px 12px',
    color: '#2D332D',
    fontVariantNumeric: 'tabular-nums',
  },
  sensorRow: {
    background: 'rgba(255,255,255,0.5)',
    borderRadius: '8px',
    transition: 'all 0.15s',
  },
  loading: {
    textAlign: 'center',
    padding: '30px',
    color: '#8A928A',
    fontSize: '14px',
  },
  gaugeRow: {
    display: 'flex',
    justifyContent: 'center',
    gap: '24px',
    marginBottom: '24px',
    flexWrap: 'wrap',
  },
};

const classStyleMap = {
  SYS_1: { bg: '#E8F5E9', text: '#1B5E20' },
  SYS_2: { bg: '#E3F2FD', text: '#0D47A1' },
  SYS_3: { bg: '#FFF3E0', text: '#E65100' },
  SYS_4: { bg: '#F3E5F5', text: '#4A148C' },
  PROCESS: { bg: '#F3E5F5', text: '#4A148C' },
  INSTRUMENT: { bg: '#E3F2FD', text: '#0D47A1' },
};

const parseUtcMs = (value) => {
  if (value == null) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T');
  const ms = Date.parse(normalized);
  return Number.isNaN(ms) ? null : ms;
};

function BetaAlertDetailModal({ alert, onClose }) {
  const [sensorAlerts, setSensorAlerts] = useState([]);
  const [minuteAlerts, setMinuteAlerts] = useState([]);
  const [flowData, setFlowData] = useState(null);
  const [standaloneMinutes, setStandaloneMinutes] = useState([]);
  const [loading, setLoading] = useState(true);

  const isStandalone = !!(alert && alert.is_standalone);

  useEffect(() => {
    if (!alert) return;
    setLoading(true);

    if (alert.is_standalone) {
      // Standalone sensor: fetch minute-by-minute alarm detail
      getBetaStandaloneAlarmDetail(alert.class, alert.start_ts, alert.end_ts)
        .then((res) => {
          setStandaloneMinutes(res.data.minute_rows || []);
          setSensorAlerts([]);
          setMinuteAlerts([]);
          setFlowData({});
        })
        .catch(() => {
          setStandaloneMinutes([]);
          setSensorAlerts([]);
          setMinuteAlerts([]);
          setFlowData({});
        })
        .finally(() => setLoading(false));
      return;
    }

    Promise.allSettled([
      getBetaAlertsSensorLevel({
        start_ts: alert.start_ts,
        end_ts: alert.end_ts,
        class: alert.class,
        alarm_view: alert.view_type || 'minute',
      }),
      getBetaAlerts({
        alarm_view: 'minute',
        class: alert.class,
      }),
      getBetaRiskDecompositionForEpisode(alert.start_ts, alert.end_ts),
    ])
      .then(([sensorRes, minuteRes, decompRes]) => {
        if (sensorRes.status === 'fulfilled') {
          setSensorAlerts(sensorRes.value.data.sensor_alerts || []);
        } else {
          setSensorAlerts([]);
        }
        if (minuteRes.status === 'fulfilled') {
          setMinuteAlerts(minuteRes.value.data.alerts || []);
        } else {
          setMinuteAlerts([]);
        }
        if (decompRes.status === 'fulfilled') {
          setFlowData(decompRes.value.data.flow_data || {});
        } else {
          setFlowData({});
        }
        setStandaloneMinutes([]);
      })
      .finally(() => setLoading(false));
  }, [alert]);

  const hasDecomposition = useMemo(() => {
    if (!flowData) return false;
    const subsystems = flowData.subsystems || {};
    return Object.keys(subsystems).length > 0;
  }, [flowData]);

  if (!alert) return null;
  const currentView = alert.view_type || 'minute';
  const isSpanView = currentView === 'span';

  const severityStyle = alert.severity === 'HIGH'
    ? { bg: '#FFCDD2', text: '#C62828' }
    : alert.severity === 'MEDIUM'
      ? { bg: '#FFE0B2', text: '#E65100' }
      : alert.severity === 'MIXED'
        ? { bg: '#ECEFF1', text: '#455A64' }
      : { bg: '#FFF8E1', text: '#9A6A00' };
  const classStyle = classStyleMap[alert.class] || { bg: '#E6F4EA', text: '#1B5E20' };
  const displayWindow = !alert.end_ts || String(alert.start_ts) === String(alert.end_ts)
    ? formatTimestamp(alert.start_ts)
    : `${formatTimestamp(alert.start_ts)} -- ${formatTimestamp(alert.end_ts)}`;
  const severityMixLabel = alert.severity_mix || [
    alert.high_count ? `${alert.high_count} high` : null,
    alert.medium_count ? `${alert.medium_count} medium` : null,
    alert.low_count ? `${alert.low_count} low` : null,
  ].filter(Boolean).join(', ');
  const spanMeanContributionDenom = useMemo(() => {
    if (!isSpanView || !sensorAlerts.length) return 0;
    return sensorAlerts.reduce((sum, row) => sum + (Number(row.sensor_mean_score) || 0), 0);
  }, [isSpanView, sensorAlerts]);
  const minuteRowsInSpan = useMemo(() => {
    if (!isSpanView || !minuteAlerts.length) return [];
    const spanStart = parseUtcMs(alert.start_ts || alert.event_start);
    const spanEnd = parseUtcMs(alert.end_ts || alert.event_end || alert.start_ts || alert.event_start);
    if (spanStart == null || spanEnd == null) return [];
    return minuteAlerts
      .filter((row) => row.class === alert.class && row.class !== 'NORMAL')
      .filter((row) => {
        const rowStart = parseUtcMs(row.start_ts || row.event_start);
        const rowEnd = parseUtcMs(row.end_ts || row.event_end || row.start_ts || row.event_start);
        return rowStart != null && rowEnd != null && rowStart <= spanEnd && rowEnd >= spanStart;
      })
      .sort((a, b) => {
        const aTs = String(a.start_ts || a.event_start || '');
        const bTs = String(b.start_ts || b.event_start || '');
        return aTs.localeCompare(bTs);
      });
  }, [isSpanView, minuteAlerts, alert]);

  return (
    <AnimatePresence>
      <motion.div
        style={styles.overlay}
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        onClick={onClose}
      >
        <motion.div
          style={styles.modal}
          initial={{ opacity: 0, scale: 0.94, y: 24 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.94, y: 24 }}
          transition={{ duration: 0.35, ease: [0.4, 0, 0.2, 1] }}
          onClick={(event) => event.stopPropagation()}
        >
          <div style={styles.header}>
            <div style={styles.titleArea}>
              <div style={styles.title}>
                {isStandalone
                  ? 'Sensor Anomaly Span Detail'
                  : currentView === 'span' ? 'System Anomaly Span Detail' : 'System Anomaly Detail'}
                <InfoTooltip text={isStandalone
                  ? 'This detail view shows the minute-by-minute alarm rows for the selected standalone sensor anomaly span, including severity and score values at each alarm minute.'
                  : currentView === 'span'
                  ? 'This detail view summarizes the selected dynamic anomaly span, including how many minute-level anomalies it contains, the severity mix inside the span, and the aggregated sensor contributions across that span.'
                  : 'This detail view summarizes the selected system anomaly row. Risk values are subsystem-level scores for that minute, while sensor contribution values show relative sensor influence and do not sum to the risk score.'} />
              </div>
              <div style={styles.subtitle}>
                {displayWindow}
              </div>
            </div>
            <motion.button
              style={styles.closeBtn}
              onClick={onClose}
              whileHover={{ background: '#FFCDD2', borderColor: '#EF9A9A', color: '#C62828' }}
            >
              x
            </motion.button>
          </div>

          <div style={styles.badges}>
            <span style={styles.badge(severityStyle.bg, severityStyle.text)}>{alert.severity}</span>
            <span style={styles.badge(classStyle.bg, classStyle.text)}>{alert.class}</span>
            <span style={styles.badge('#E6F4EA', '#1B5E20')}>{alert.threshold || 'ADAPTIVE'} THRESHOLD</span>
          </div>

          {/* Centered gauge + stats grid */}
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', marginBottom: '24px' }}>
            <GaugeWidget
              value={isSpanView ? alert.peak_risk_score : alert.risk_score}
              max={Math.max((isSpanView ? alert.peak_risk_score : alert.risk_score) || 0.01, alert.adaptive_threshold || 0.01) * 1.2}
              label={isSpanView ? 'Peak Risk Score' : 'Risk Score'}
              color={alert.severity === 'HIGH' ? '#EF5350' : alert.severity === 'MEDIUM' ? '#FFA726' : '#4CAF50'}
              size={130}
              sublabel={alert.adaptive_threshold != null ? `Threshold: ${Number(alert.adaptive_threshold).toFixed(3)}` : undefined}
              tooltip="Normalized risk score (0-1) used by the anomaly system. The anomaly fires when this exceeds the adaptive threshold."
            />

            {/* Stats grid below gauge */}
            {/* Row 1: Quality Metrics */}
            <div style={{ display: 'flex', gap: '8px', justifyContent: 'center', marginTop: '16px', width: '100%' }}>
              {alert.system_confidence != null && (
                <div style={{ ...styles.metaItem, flex: '1 1 0', minWidth: 0, textAlign: 'center' }}>
                  <div style={styles.metaLabel}>
                    Confidence <InfoTooltip text="System confidence in the anomaly detection model at the time of this anomaly." />
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '6px', justifyContent: 'center' }}>
                    <div style={{ flex: 1, height: '6px', background: '#E8ECE8', borderRadius: '3px', overflow: 'hidden', maxWidth: '60px' }}>
                      <div style={{
                        width: `${(alert.system_confidence || 0) * 100}%`,
                        height: '100%',
                        background: alert.system_confidence >= 0.8 ? '#4CAF50' : alert.system_confidence >= 0.5 ? '#FFA726' : '#EF5350',
                        borderRadius: '3px',
                      }} />
                    </div>
                    <span style={styles.metaValue}>{((alert.system_confidence || 0) * 100).toFixed(0)}%</span>
                  </div>
                </div>
              )}
            </div>
            {/* Row 2: Alarm Metadata */}
            <div style={{ display: 'flex', gap: '8px', justifyContent: 'center', marginTop: '8px', width: '100%' }}>
              <div style={{ ...styles.metaItem, flex: '1 1 0', minWidth: 0, textAlign: 'center' }}>
                <div style={styles.metaLabel}>{currentView === 'span' ? 'Span' : 'Window'}</div>
                <div style={styles.metaValue}>{formatDuration(alert.duration_minutes)}</div>
              </div>
              {currentView === 'span' && (
                <div style={{ ...styles.metaItem, flex: '1 1 0', minWidth: 0, textAlign: 'center' }}>
                  <div style={styles.metaLabel}>Alarm Minutes</div>
                  <div style={styles.metaValue}>{isStandalone ? standaloneMinutes.length : minuteRowsInSpan.length}</div>
                </div>
              )}
              {!isStandalone && (
                <div style={{ ...styles.metaItem, flex: '1 1 0', minWidth: 0, textAlign: 'center' }}>
                  <div style={styles.metaLabel}>Sensors</div>
                  <div style={styles.metaValue}>{alert.affected_sensor_count}</div>
                </div>
              )}
              {currentView === 'span' && severityMixLabel && (
                <div style={{ ...styles.metaItem, flex: '1 1 0', minWidth: 0, textAlign: 'center' }}>
                  <div style={styles.metaLabel}>Severity Mix</div>
                  <div style={{ ...styles.metaValue, fontSize: '13px' }}>{severityMixLabel}</div>
                </div>
              )}
            </div>
          </div>

          {/* Primary Sensor Callout */}
          {!loading && sensorAlerts.length > 0 && (() => {
            const topSensor = sensorAlerts.slice().sort((a, b) => (a.sensor_rank || 99) - (b.sensor_rank || 99))[0];
            if (!topSensor) return null;
            const pct = topSensor.sensor_contribution_pct;
            return (
              <div style={{
                background: 'linear-gradient(135deg, rgba(27,94,32,0.06) 0%, rgba(76,175,80,0.04) 100%)',
                border: '1px solid rgba(27,94,32,0.15)',
                borderRadius: '14px',
                padding: '16px 20px',
                marginBottom: '20px',
                display: 'flex',
                alignItems: 'center',
                gap: '16px',
              }}>
                <div style={{
                  width: '40px',
                  height: '40px',
                  borderRadius: '12px',
                  background: alert.severity === 'HIGH' ? '#FFCDD2' : alert.severity === 'MEDIUM' ? '#FFE0B2' : '#E8F5E9',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  fontSize: '18px',
                  flexShrink: 0,
                }}>
                  {'\u26A0'}
                </div>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: '14px', fontWeight: 600, color: '#1A1F1A', marginBottom: '4px' }}>
                    {formatSensorName(topSensor.sensor)}
                    <span style={{ fontSize: '11px', fontWeight: 400, color: '#6B736B', marginLeft: '8px' }}>Primary Driver</span>
                  </div>
                  <div style={{ display: 'flex', gap: '6px', alignItems: 'center', flexWrap: 'wrap' }}>
                    {pct != null && (
                      <span style={{ fontSize: '12px', color: '#4A524A' }}>
                        <strong>{pct}%</strong> contribution
                      </span>
                    )}
                    {pct != null && <span style={{ color: '#D0D4D0' }}>|</span>}
                    <span style={{
                      padding: '1px 6px',
                      borderRadius: '8px',
                      fontSize: '10px',
                      fontWeight: 600,
                      background: topSensor.severity === 'HIGH' ? '#FFCDD2' : topSensor.severity === 'MEDIUM' ? '#FFE0B2' : '#FFF8E1',
                      color: topSensor.severity === 'HIGH' ? '#C62828' : topSensor.severity === 'MEDIUM' ? '#E65100' : '#9A6A00',
                    }}>
                      {topSensor.severity}
                    </span>
                  </div>
                </div>
                {pct != null && (
                  <div style={{
                    textAlign: 'center',
                    flexShrink: 0,
                    background: 'rgba(27,94,32,0.08)',
                    border: '1px solid rgba(27,94,32,0.15)',
                    borderRadius: '12px',
                    padding: '8px 16px',
                    minWidth: '80px',
                  }}>
                    <div style={{ fontSize: '22px', fontWeight: 700, color: '#1B5E20', lineHeight: 1.1 }}>{pct}%</div>
                    <div style={{ fontSize: '9px', color: '#4A6A4A', textTransform: 'uppercase', letterSpacing: '0.05em', marginTop: '2px', fontWeight: 600 }}>contribution</div>
                  </div>
                )}
              </div>
            );
          })()}

          {loading ? (
            <div style={styles.loading}>{isStandalone ? 'Loading sensor anomaly details...' : 'Loading system anomaly details...'}</div>
          ) : isStandalone ? (
            <>
              <div style={styles.sectionTitle}>
                Minute-by-Minute Alarm Rows ({standaloneMinutes.length})
                <InfoTooltip text="Each row is a minute where this sensor was in alarm state within the selected span. Severity and score values reflect the standalone analysis pipeline output at that minute." />
              </div>
              {standaloneMinutes.length === 0 ? (
                <div style={styles.loading}>No minute-level alarm rows were returned for this span.</div>
              ) : (
                <div style={{ overflowX: 'auto' }}>
                  <table style={styles.sensorTable}>
                    <thead>
                      <tr>
                        <th style={styles.th}>Timestamp (UTC)</th>
                        <th style={styles.th}>Severity</th>
                        <th style={styles.th}>Evidence</th>
                        <th style={styles.th}>SQS</th>
                      </tr>
                    </thead>
                    <tbody>
                      {standaloneMinutes.map((row, idx) => {
                        const rowSeverity = String(row.severity || 'LOW').toUpperCase();
                        return (
                          <motion.tr
                            key={`${row.timestamp_utc}-${idx}`}
                            style={styles.sensorRow}
                            whileHover={{ background: 'rgba(168, 220, 168, 0.15)' }}
                          >
                            <td style={styles.td}>{formatTimestamp(row.timestamp_utc)}</td>
                            <td style={styles.td}>
                              <span style={{
                                padding: '2px 8px',
                                borderRadius: '10px',
                                fontSize: '10px',
                                fontWeight: 600,
                                background: rowSeverity === 'HIGH' ? '#FFCDD2' : rowSeverity === 'MEDIUM' ? '#FFE0B2' : '#FFF8E1',
                                color: rowSeverity === 'HIGH' ? '#C62828' : rowSeverity === 'MEDIUM' ? '#E65100' : '#9A6A00',
                              }}>
                                {rowSeverity}
                              </span>
                            </td>
                            <td style={styles.td}>{row.evidence != null ? formatScore(row.evidence) : '--'}</td>
                            <td style={styles.td}>{row.sqs != null ? formatScore(row.sqs) : '--'}</td>
                          </motion.tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          ) : (
            <>
              {hasDecomposition && (
                <SensorFlowDecomposition
                  flowData={flowData}
                  alertClass={alert.class}
                  interactive={false}
                />
              )}

              <div style={styles.sectionTitle}>
                Sensor-Level Anomalies ({sensorAlerts.length})
                <InfoTooltip text={currentView === 'span'
                  ? 'These rows aggregate sensor involvement across the selected anomaly span. Peak contribution is the highest minute-level contribution, and mean contribution is averaged over the full span.'
                  : 'These rows show per-sensor contribution values for the selected anomaly minute. These contribution values are ranked relative influences and are not expected to sum to the subsystem risk score.'} />
              </div>

              {sensorAlerts.length === 0 ? (
                <div style={styles.loading}>No sensor-level rows were returned for this {currentView === 'span' ? 'span' : 'anomaly'}.</div>
              ) : (
                <div style={{ overflowX: 'auto' }}>
                  <table style={styles.sensorTable}>
                    <thead>
                      <tr>
                        <th style={styles.th}>Rank</th>
                        <th style={styles.th}>Sensor</th>
                        <th style={styles.th}>Contribution</th>
                        {isSpanView && <th style={styles.th}>Mean %</th>}
                        <th style={styles.th}>Severity</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sensorAlerts
                        .slice()
                        .sort((a, b) => (a.sensor_rank || 99) - (b.sensor_rank || 99))
                        .map((sensorAlert, idx) => (
                          <motion.tr
                            key={`${sensorAlert.sensor}-${idx}`}
                            style={styles.sensorRow}
                            whileHover={{ background: 'rgba(168, 220, 168, 0.15)' }}
                          >
                            <td style={styles.td}>#{sensorAlert.sensor_rank}</td>
                            <td style={{ ...styles.td, fontWeight: 500 }}>{formatSensorName(sensorAlert.sensor)}</td>
                            <td style={styles.td}>
                              <span style={{ fontWeight: 600 }}>{sensorAlert.sensor_contribution_pct != null ? `${sensorAlert.sensor_contribution_pct}%` : '--'}</span>
                            </td>
                            {isSpanView && (
                              <td style={styles.td}>
                                {spanMeanContributionDenom > 0 && sensorAlert.sensor_mean_score != null
                                  ? `${(((Number(sensorAlert.sensor_mean_score) || 0) / spanMeanContributionDenom) * 100).toFixed(1)}%`
                                  : '--'}
                              </td>
                            )}
                            <td style={styles.td}>
                              <span style={{
                                padding: '2px 8px',
                                borderRadius: '10px',
                                fontSize: '10px',
                                fontWeight: 600,
                                background: sensorAlert.severity === 'HIGH'
                                  ? '#FFCDD2'
                                  : sensorAlert.severity === 'MEDIUM'
                                    ? '#FFE0B2'
                                    : sensorAlert.severity === 'MIXED'
                                      ? '#ECEFF1'
                                    : '#FFF8E1',
                                color: sensorAlert.severity === 'HIGH'
                                  ? '#C62828'
                                  : sensorAlert.severity === 'MEDIUM'
                                    ? '#E65100'
                                    : sensorAlert.severity === 'MIXED'
                                      ? '#455A64'
                                    : '#9A6A00',
                              }}>
                                {sensorAlert.severity}
                              </span>
                            </td>
                          </motion.tr>
                        ))}
                    </tbody>
                  </table>
                </div>
              )}

              {isSpanView && (
                <>
                  <div style={styles.sectionTitle}>
                    Minute-by-Minute Anomalies ({minuteRowsInSpan.length})
                    <InfoTooltip text="Raw minute-level anomaly rows inside this selected span. These are direct minute entries, not aggregated totals." />
                  </div>
                  {minuteRowsInSpan.length === 0 ? (
                    <div style={styles.loading}>No minute-level anomaly rows were returned for this span.</div>
                  ) : (
                    <div style={{ overflowX: 'auto' }}>
                      <table style={styles.sensorTable}>
                        <thead>
                          <tr>
                            <th style={styles.th}>Window (UTC)</th>
                            <th style={styles.th}>Severity</th>
                            <th style={styles.th}>Risk Score</th>
                            <th style={styles.th}>Confidence</th>
                            <th style={styles.th}>Primary Sensor</th>
                          </tr>
                        </thead>
                        <tbody>
                          {minuteRowsInSpan.map((row, idx) => {
                            const rowSeverity = String(row.severity || 'LOW').toUpperCase();
                            const rowWindow = !row.end_ts || String(row.start_ts) === String(row.end_ts)
                              ? formatTimestamp(row.start_ts || row.event_start)
                              : `${formatTimestamp(row.start_ts || row.event_start)} -- ${formatTimestamp(row.end_ts || row.event_end)}`;
                            return (
                              <motion.tr
                                key={`${row.start_ts || row.event_start}-${idx}`}
                                style={styles.sensorRow}
                                whileHover={{ background: 'rgba(168, 220, 168, 0.15)' }}
                              >
                                <td style={styles.td}>{rowWindow}</td>
                                <td style={styles.td}>
                                  <span style={{
                                    padding: '2px 8px',
                                    borderRadius: '10px',
                                    fontSize: '10px',
                                    fontWeight: 600,
                                    background: rowSeverity === 'HIGH'
                                      ? '#FFCDD2'
                                      : rowSeverity === 'MEDIUM'
                                        ? '#FFE0B2'
                                        : '#FFF8E1',
                                    color: rowSeverity === 'HIGH'
                                      ? '#C62828'
                                      : rowSeverity === 'MEDIUM'
                                        ? '#E65100'
                                        : '#9A6A00',
                                  }}>
                                    {rowSeverity}
                                  </span>
                                </td>
                                <td style={styles.td}>
                                  {formatScore(row.risk_score)}
                                  {row.adaptive_threshold != null && (
                                    <span style={{ fontSize: '11px', color: '#6B736B', marginLeft: '4px' }}>
                                      / {formatScore(row.adaptive_threshold)}
                                    </span>
                                  )}
                                </td>
                                <td style={styles.td}>
                                  {row.system_confidence != null ? `${((row.system_confidence || 0) * 100).toFixed(0)}%` : '--'}
                                </td>
                                <td style={{ ...styles.td, fontWeight: 500 }}>{formatSensorName(row.sensor_id)}</td>
                              </motion.tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  )}
                </>
              )}
            </>
          )}
        </motion.div>
      </motion.div>
    </AnimatePresence>
  );
}

export default BetaAlertDetailModal;
