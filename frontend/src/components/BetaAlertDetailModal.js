import React, { useEffect, useMemo, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  getBetaAlertsSensorLevel,
  getBetaRiskDecompositionForEpisode,
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

function BetaAlertDetailModal({ alert, onClose }) {
  const [sensorAlerts, setSensorAlerts] = useState([]);
  const [flowData, setFlowData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!alert) return;
    setLoading(true);
    Promise.allSettled([
      getBetaAlertsSensorLevel({
        start_ts: alert.start_ts,
        end_ts: alert.end_ts,
        class: alert.class,
        alarm_view: alert.view_type || 'minute',
      }),
      getBetaRiskDecompositionForEpisode(alert.start_ts, alert.end_ts),
    ])
      .then(([sensorRes, decompRes]) => {
        if (sensorRes.status === 'fulfilled') {
          setSensorAlerts(sensorRes.value.data.sensor_alerts || []);
        } else {
          setSensorAlerts([]);
        }
        if (decompRes.status === 'fulfilled') {
          setFlowData(decompRes.value.data.flow_data || {});
        } else {
          setFlowData({});
        }
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
                {currentView === 'span' ? 'System Alarm Span Detail' : 'System Alarm Detail'}
                <InfoTooltip text={currentView === 'span'
                  ? 'This detail view summarizes the selected dynamic alarm span, including how many minute-level alerts it contains, the severity mix inside the span, and the aggregated sensor contributions across that span.'
                  : 'This detail view summarizes the selected system alarm row. Risk values are subsystem-level scores for that minute, while sensor contribution values show relative sensor influence and do not sum to the risk score.'} />
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

          <div style={styles.gaugeRow}>
            <GaugeWidget
              value={alert.max_score}
              max={Math.max(alert.max_score || 1, alert.sensor_max_score || 1)}
              label={isSpanView ? 'Peak Contribution Sum' : 'Contribution Sum'}
              color={alert.severity === 'HIGH' ? '#EF5350' : alert.severity === 'MEDIUM' ? '#FFA726' : '#4CAF50'}
              size={120}
              tooltip={isSpanView
                ? 'Highest raw sum of all sensor contributions reached anywhere in the alarm span.'
                : 'Raw sum of all sensor contributions at the selected alarm minute.'}
            />
            {isSpanView && (
              <GaugeWidget
                value={alert.mean_score}
                max={Math.max(alert.max_score || 1, alert.sensor_max_score || 1)}
                label="Mean Contribution Sum"
                color={alert.severity === 'HIGH' ? '#EF5350' : alert.severity === 'MEDIUM' ? '#FFA726' : '#4CAF50'}
                size={120}
                tooltip="Mean raw sum of all sensor contributions across the alarm span."
              />
            )}
            <GaugeWidget
              value={alert.sensor_max_score}
              max={Math.max(alert.max_score || 1, alert.sensor_max_score || 1)}
              label="Top Sensor Contribution"
              color="#388E3C"
              size={120}
              tooltip={currentView === 'span'
                ? 'Peak individual sensor contribution across the span (sigma-scaled residual).'
                : 'Top individual sensor contribution at the selected alarm minute (sigma-scaled residual).'}
            />
          </div>

          <div style={styles.metaGrid}>
            <div style={styles.metaItem}>
              <div style={styles.metaLabel}>
                {currentView === 'span' ? 'Span' : 'Window'}
                <InfoTooltip text={currentView === 'span'
                  ? 'Dynamic alarm spans run from the first contiguous alarm minute to the last contiguous alarm minute in the cluster.'
                  : 'Timestamp-level alarm rows are shown with a one-minute window.'} />
              </div>
              <div style={styles.metaValue}>{formatDuration(alert.duration_minutes)}</div>
            </div>
            <div style={styles.metaItem}>
              <div style={styles.metaLabel}>
                Primary Sensor
                <InfoTooltip text="Top-ranked sensor at this alarm timestamp, based on the source contribution data." />
              </div>
              <div style={{ ...styles.metaValue, fontSize: '13px' }}>{formatSensorName(alert.sensor_id)}</div>
            </div>
            <div style={styles.metaItem}>
              <div style={styles.metaLabel}>{currentView === 'span' ? 'Alerts In Span' : 'Affected Sensors'}</div>
              <div style={styles.metaValue}>{currentView === 'span' ? (alert.minute_count || 1) : alert.affected_sensor_count}</div>
            </div>
            <div style={styles.metaItem}>
              <div style={styles.metaLabel}>{isSpanView ? 'Peak Contribution Sum' : 'Contribution Sum'}</div>
              <div style={styles.metaValue}>{formatScore(alert.max_score)}</div>
            </div>
            {isSpanView && (
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>Mean Contribution Sum</div>
                <div style={styles.metaValue}>{formatScore(alert.mean_score)}</div>
              </div>
            )}
            <div style={styles.metaItem}>
              <div style={styles.metaLabel}>{currentView === 'span' ? 'Severity Mix' : 'Top Sensor Contribution'}</div>
              <div style={styles.metaValue}>{currentView === 'span' ? (severityMixLabel || '--') : formatScore(alert.sensor_max_score)}</div>
            </div>
            {currentView === 'span' && (
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>Affected Sensors</div>
                <div style={styles.metaValue}>{alert.affected_sensor_count}</div>
              </div>
            )}
            {currentView === 'span' && (
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>Top Sensor Contribution</div>
                <div style={styles.metaValue}>{formatScore(alert.sensor_max_score)}</div>
              </div>
            )}
          </div>

          {loading ? (
            <div style={styles.loading}>Loading system alarm details...</div>
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
                Sensor-Level Alerts ({sensorAlerts.length})
                <InfoTooltip text={currentView === 'span'
                  ? 'These rows aggregate sensor involvement across the selected alarm span. Peak contribution is the highest minute-level contribution, and mean contribution is averaged over the full span.'
                  : 'These rows show per-sensor contribution values for the selected alarm minute. These contribution values are ranked relative influences and are not expected to sum to the subsystem risk score.'} />
              </div>

              {sensorAlerts.length === 0 ? (
                <div style={styles.loading}>No sensor-level rows were returned for this {currentView === 'span' ? 'span' : 'alert'}.</div>
              ) : (
                <div style={{ overflowX: 'auto' }}>
                  <table style={styles.sensorTable}>
                    <thead>
                      <tr>
                        <th style={styles.th}>Rank</th>
                        <th style={styles.th}>Sensor</th>
                        <th style={styles.th}>{isSpanView ? 'Peak Contribution' : 'Contribution'}</th>
                        {isSpanView && <th style={styles.th}>Mean Contribution</th>}
                        <th style={styles.th}>Severity</th>
                        <th style={styles.th}>Class</th>
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
                            <td style={styles.td}>{formatScore(sensorAlert.sensor_peak_score)}</td>
                            {isSpanView && <td style={styles.td}>{formatScore(sensorAlert.sensor_mean_score)}</td>}
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
                            <td style={styles.td}>{sensorAlert.class}</td>
                          </motion.tr>
                        ))}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}
        </motion.div>
      </motion.div>
    </AnimatePresence>
  );
}

export default BetaAlertDetailModal;
