import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { getRiskDecompositionForEpisode, getAlertsSensorLevel } from '../utils/api';
import { formatTimestamp, formatDuration, formatSensorName, formatScore } from '../utils/formatters';
import GaugeWidget from './GaugeWidget';
import SensorFlowDecomposition from './SensorFlowDecomposition';
import SensorDetailModal from './SensorDetailModal';
import InfoTooltip from './InfoTooltip';

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
    zIndex: 1000,
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
    color: color,
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
    cursor: 'pointer',
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

function AlertDetailModal({ alert, onClose }) {
  const [flowData, setFlowData] = useState(null);
  const [sensorAlerts, setSensorAlerts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedSensor, setSelectedSensor] = useState(null);

  useEffect(() => {
    if (!alert) return;
    setLoading(true);
    Promise.all([
      getRiskDecompositionForEpisode(alert.start_ts, alert.end_ts),
      getAlertsSensorLevel({ start_ts: alert.start_ts, end_ts: alert.end_ts, class: alert.class }),
    ])
      .then(([decompRes, sensorRes]) => {
        setFlowData(decompRes.data.flow_data);
        setSensorAlerts(sensorRes.data.sensor_alerts || []);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [alert]);

  if (!alert) return null;

  const severityStyle = alert.severity === 'HIGH' ? { bg: '#FFCDD2', text: '#C62828' } : { bg: '#FFE0B2', text: '#E65100' };
  const classStyle = { MECH: { bg: '#E8F5E9', text: '#1B5E20' }, ELEC: { bg: '#E0F2F1', text: '#004D40' }, THERM: { bg: '#FFF3E0', text: '#E65100' }, PROCESS: { bg: '#F3E5F5', text: '#4A148C' }, INSTRUMENT: { bg: '#E3F2FD', text: '#0D47A1' } }[alert.class] || { bg: '#E6F4EA', text: '#1B5E20' };

  return (
    <>
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
            onClick={(e) => e.stopPropagation()}
          >
            <div style={styles.header}>
              <div style={styles.titleArea}>
                <div style={styles.title}>
                  Alert Episode Detail
                  <InfoTooltip text="Each alert episode is a merged contiguous period where risk_score exceeded a threshold. The primary sensor is the top-ranked sensor by peak anomaly score within the episode window. Affected sensors are ranked by peak then mean score." />
                </div>
                <div style={styles.subtitle}>
                  {formatTimestamp(alert.start_ts)} -- {formatTimestamp(alert.end_ts)}
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
              <span style={styles.badge('#E6F4EA', '#1B5E20')}>{alert.threshold} THRESHOLD</span>
            </div>

            <div style={styles.gaugeRow}>
              <GaugeWidget
                value={alert.max_score} label="Peak Risk Score"
                color={alert.max_score >= 0.8 ? '#EF5350' : '#FFA726'} size={120}
                tooltip="Maximum risk_score within this episode window. Severity is HIGH if >= 0.85."
              />
              <GaugeWidget
                value={alert.mean_score} label="Mean Risk Score"
                color={alert.mean_score >= 0.8 ? '#EF5350' : alert.mean_score >= 0.55 ? '#FFA726' : '#4CAF50'} size={120}
                tooltip="Average risk_score across all timestamps in the episode."
              />
              <GaugeWidget
                value={alert.sensor_max_score} label="Top Sensor Score"
                color="#388E3C" size={120}
                tooltip="Peak anomaly score of the top-ranked sensor from the merged Engine A/B sensor score matrix."
              />
            </div>

            <div style={styles.metaGrid}>
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>
                  Duration
                  <InfoTooltip text="Total duration of the merged episode in minutes. Gaps <= 3 minutes between contiguous segments are merged." />
                </div>
                <div style={styles.metaValue}>{formatDuration(alert.duration_minutes)}</div>
              </div>
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>
                  Primary Sensor
                  <InfoTooltip text="Sensor with highest peak score in the episode. Selected from class-specific candidate sensors (e.g., MECH uses vibration tags, ELEC uses electrical tags)." />
                </div>
                <div style={{ ...styles.metaValue, fontSize: '13px' }}>{formatSensorName(alert.sensor_id)}</div>
              </div>
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>Affected Sensors</div>
                <div style={styles.metaValue}>{alert.affected_sensor_count}</div>
              </div>
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>
                  Peak Risk
                  <InfoTooltip text="Max of risk_score (fused output) over the episode window." right />
                </div>
                <div style={styles.metaValue}>{formatScore(alert.max_score)}</div>
              </div>
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>Mean Risk</div>
                <div style={styles.metaValue}>{formatScore(alert.mean_score)}</div>
              </div>
              <div style={styles.metaItem}>
                <div style={styles.metaLabel}>
                  Sensor Peak Score
                  <InfoTooltip text="Maximum value from the merged Engine A/B sensor score matrix for the top sensor across the episode timespan." right />
                </div>
                <div style={styles.metaValue}>{formatScore(alert.sensor_max_score)}</div>
              </div>
            </div>

            {loading ? (
              <div style={styles.loading}>Loading decomposition data...</div>
            ) : (
              <>
                <SensorFlowDecomposition flowData={flowData} onSensorClick={(id) => setSelectedSensor(id)} />

                {sensorAlerts.length > 0 && (
                  <>
                    <div style={styles.sectionTitle}>
                      Sensor-Level Alerts ({sensorAlerts.length})
                      <InfoTooltip text="Per-sensor alert rows for this episode. Each sensor is ranked by peak then mean anomaly score. Candidate sensors depend on classification: MECH uses vibration tags, ELEC uses electrical tags, PROCESS uses vibration + electrical, INSTRUMENT uses core identity signals (KW, PF, VLL, IAVG, FREC) + electrical." />
                    </div>
                    <div style={{ overflowX: 'auto' }}>
                      <table style={styles.sensorTable}>
                        <thead>
                          <tr>
                            <th style={styles.th}>Rank</th>
                            <th style={styles.th}>Sensor</th>
                            <th style={styles.th}>Peak Score</th>
                            <th style={styles.th}>Mean Score</th>
                            <th style={styles.th}>Severity</th>
                            <th style={styles.th}>Class</th>
                          </tr>
                        </thead>
                        <tbody>
                          {sensorAlerts
                            .sort((a, b) => (a.sensor_rank || 99) - (b.sensor_rank || 99))
                            .map((sa, i) => (
                              <motion.tr
                                key={i}
                                style={styles.sensorRow}
                                whileHover={{ background: 'rgba(168, 220, 168, 0.15)' }}
                                onClick={() => setSelectedSensor(sa.sensor)}
                              >
                                <td style={styles.td}>#{sa.sensor_rank}</td>
                                <td style={{ ...styles.td, fontWeight: 500 }}>{formatSensorName(sa.sensor)}</td>
                                <td style={styles.td}>{formatScore(sa.sensor_peak_score)}</td>
                                <td style={styles.td}>{formatScore(sa.sensor_mean_score)}</td>
                                <td style={styles.td}>
                                  <span style={{ padding: '2px 8px', borderRadius: '10px', fontSize: '10px', fontWeight: 600, background: sa.severity === 'HIGH' ? '#FFCDD2' : '#FFE0B2', color: sa.severity === 'HIGH' ? '#C62828' : '#E65100' }}>
                                    {sa.severity}
                                  </span>
                                </td>
                                <td style={styles.td}>{sa.class}</td>
                              </motion.tr>
                            ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                )}
              </>
            )}
          </motion.div>
        </motion.div>
      </AnimatePresence>

      {selectedSensor && (
        <SensorDetailModal sensorId={selectedSensor} onClose={() => setSelectedSensor(null)} />
      )}
    </>
  );
}

export default AlertDetailModal;